import * as openai from "@derouter/protocol-openai";
import {
  ConsumerCreateJobError,
  ConsumerOpenJobConnectionError,
  RPC,
  type OfferRemoved,
  type OfferSnapshot,
  type ProviderHeartbeat,
  type ProviderRecord,
} from "@derouter/rpc";
import { readCborOnce, unreachable } from "@derouter/rpc/util";
import bodyParser from "body-parser";
import { and, eq, gte, lte } from "drizzle-orm";
import { toMilliseconds } from "duration-fns";
import express, { type Request, type Response } from "express";
import deepEqual from "fast-deep-equal";
import pRetry from "p-retry";
import pTimeout, { TimeoutError } from "p-timeout";
import * as v from "valibot";
import { ConfigSchema } from "./config.js";
import { d } from "./lib/drizzle.js";
import { parseWeiToEth, safeTryAsync } from "./lib/util.js";

enum Currency {
  Polygon,
}

export class OpenAiConsumer {
  private _rpc: RPC;

  constructor(readonly config: v.InferOutput<typeof ConfigSchema>) {
    this._rpc = new RPC(config.rpc.host, config.rpc.port);

    this._rpc.emitter.on("providerUpdated", (e) => this.onProviderUpdated(e));
    this._rpc.emitter.on("providerHeartbeat", (e) =>
      this.onProviderHeartbeat(e),
    );
    this._rpc.emitter.on("offerUpdated", (e) => this.onOfferUpdated(e));
    this._rpc.emitter.on("offerRemoved", (e) => this.onOfferRemoved(e));

    this.sync().then(() => this.run());
  }

  private async sync() {
    const providerPeerIds = new Set<string>();
    const offerSnapshotIds = new Set<number>();

    // Query active provider peer IDs.
    //

    (await this._rpc.queryActiveProviders()).forEach((id) =>
      providerPeerIds.add(id),
    );

    // Query active offer records.
    //

    const allOffers = await this._rpc.queryActiveOffers({
      protocol_ids: [openai.ProtocolId],
    });

    allOffers.forEach((offer) => {
      // We don't need to query these records anymore.
      offerSnapshotIds.delete(offer.snapshot_id);

      // But we need its provider record.
      providerPeerIds.add(offer.provider_peer_id);
    });

    // Query offer records.
    //

    {
      const array = [...offerSnapshotIds.values()];
      const batchSize = 50;

      for (let i = 0; i < array.length; i += batchSize) {
        const batch = array.slice(i, i + batchSize);

        allOffers.push(
          ...(await this._rpc.queryOfferSnapshots({
            snapshot_ids: batch,
          })),
        );
      }
    }

    // Query provider records.
    //

    const allProviders: Awaited<ReturnType<typeof this._rpc.queryProviders>> =
      [];

    {
      const array = [...providerPeerIds.values()];
      const batchSize = 50;

      for (let i = 0; i < array.length; i += batchSize) {
        const batch = array.slice(i, i + batchSize);

        allProviders.push(
          ...(await this._rpc.queryProviders({
            provider_peer_ids: batch,
          })),
        );
      }
    }

    // Insert everything into DB.
    //

    await d.db.transaction(async (tx) => {
      for (const provider of allProviders) {
        console.debug("Inserting", provider);

        console.debug(
          "Inserted",
          (
            await tx
              .insert(d.providers)
              .values({
                peerId: provider.peer_id,
                latestHeartbeatAt: provider.latest_heartbeat_at,
              })
              .returning()
          )[0],
        );
      }

      const failedOfferSnapshotIds = new Set<number>();

      offerLoop: for (const offer of allOffers) {
        console.debug("Inserting", offer);

        let json;
        try {
          json = JSON.parse(offer.protocol_payload);
        } catch (e) {
          console.error(`Failed to JSON-parse protocol payload`, e);

          failedOfferSnapshotIds.add(offer.snapshot_id);
          continue offerLoop;
        }

        const parseResult = v.safeParse(openai.OfferPayloadSchema, json);

        if (!parseResult.success) {
          console.error(
            `Failed to parse protocol payload`,
            v.flatten(parseResult.issues),
          );

          failedOfferSnapshotIds.add(offer.snapshot_id);
          continue offerLoop;
        }

        const protocolPayload = parseResult.output;

        console.debug(
          "Inserted",
          (
            await tx
              .insert(d.offerSnapshots)
              .values({
                id: offer.snapshot_id,
                providerPeerId: offer.provider_peer_id,
                providerOfferId: offer.offer_id,
                protocolId: openai.ProtocolId,
                protocolPayload,
                active: offer.active,
                modelId: protocolPayload.model_id,
                contextSize: protocolPayload.context_size,
                inputTokenPricePol: parseWeiToEth(
                  protocolPayload.input_token_price.$pol,
                ),
                outputTokenPricePol: parseWeiToEth(
                  protocolPayload.output_token_price.$pol,
                ),
              })
              .returning()
          )[0],
        );
      }
    });

    this._rpc.subscribeToActiveOffers({ protocol_ids: [openai.ProtocolId] });
    this._rpc.subscribeToActiveProviders();
  }

  private async run() {
    const app = express();

    app.get("/", (req, res) => {
      res.sendStatus(200);
    });

    app.post("/v1/completions", bodyParser.json(), (req, res) => {
      this._completionImpl(
        req,
        res,
        openai.completions.RequestBodySchema,
        openai.completions.CompletionChunkSchema,
        openai.completions.ResponseSchema,
      );
    });

    app.post("/v1/chat/completions", bodyParser.json(), (req, res) => {
      this._completionImpl(
        req,
        res,
        openai.chatCompletions.RequestBodySchema,
        openai.chatCompletions.CompletionChunkSchema,
        openai.chatCompletions.ResponseSchema,
      );
    });

    app.listen(this.config.server.port, this.config.server.host, () => {
      console.log(
        `Server listening on http://${
          this.config.server.host
        }:${this.config.server.port}`,
      );
    });
  }

  async onProviderUpdated(data: ProviderRecord) {
    console.debug("Inserting", data);

    const result = await d.db
      .insert(d.providers)
      .values({
        peerId: data.peer_id,
        latestHeartbeatAt: data.latest_heartbeat_at,
      })
      .onConflictDoUpdate({
        target: d.providers.peerId,
        set: { latestHeartbeatAt: data.latest_heartbeat_at },
      })
      .returning();

    console.debug("Inserted", result[0]);
  }

  async onProviderHeartbeat(data: ProviderHeartbeat) {
    await d.db
      .update(d.providers)
      .set({ latestHeartbeatAt: data.latest_heartbeat_at })
      .where(eq(d.providers.peerId, data.peer_id));

    console.debug("Provider heartbeat", data);
  }

  async onOfferUpdated(data: OfferSnapshot) {
    console.debug("onOfferUpdated", data);

    await d.db.transaction(async (tx) => {
      if (data.protocol_id !== openai.ProtocolId) {
        console.warn("Skipped offer with protocol", data.protocol_id);
        return;
      }

      let json;
      try {
        json = JSON.parse(data.protocol_payload);
      } catch (e) {
        console.error(`Failed to JSON-parse protocol payload`, e);
        return;
      }

      const parseResult = v.safeParse(openai.OfferPayloadSchema, json);

      if (!parseResult.success) {
        console.error(
          `Failed to parse protocol payload`,
          data.protocol_id,
          v.flatten(parseResult.issues),
        );

        return;
      }

      const providerExists = await tx.query.providers.findFirst({
        where: eq(d.providers.peerId, data.provider_peer_id),
      });

      if (!providerExists) {
        console.warn(
          `Provider ${data.provider_peer_id} not found in DB, skip offer`,
        );

        return;
      }

      const protocolPayload = parseResult.output;

      await tx
        .update(d.offerSnapshots)
        .set({ active: false })
        .where(
          and(
            eq(d.offerSnapshots.providerPeerId, data.provider_peer_id),
            eq(d.offerSnapshots.providerOfferId, data.offer_id),
            eq(d.offerSnapshots.protocolId, data.protocol_id),
          ),
        );

      let result = await tx
        .insert(d.offerSnapshots)
        .values({
          id: data.snapshot_id,
          providerPeerId: data.provider_peer_id,
          providerOfferId: data.offer_id,
          protocolId: data.protocol_id,
          protocolPayload,
          active: true,
          modelId: protocolPayload.model_id,
          contextSize: protocolPayload.context_size,
          inputTokenPricePol: parseWeiToEth(
            protocolPayload.input_token_price.$pol,
          ),
          outputTokenPricePol: parseWeiToEth(
            protocolPayload.output_token_price.$pol,
          ),
        })
        .onConflictDoUpdate({
          target: [d.offerSnapshots.id],
          set: { active: true },
        })
        .returning();

      console.debug("Inserted", result[0]);
    });
  }

  async onOfferRemoved(data: OfferRemoved) {
    if (data.protocol_id !== openai.ProtocolId) {
      console.warn("Skipped offer with protocol", data.protocol_id);
      return;
    }

    await d.db
      .update(d.offerSnapshots)
      .set({ active: false })
      .where(
        and(
          eq(d.offerSnapshots.providerPeerId, data.provider_peer_id),
          eq(d.offerSnapshots.providerOfferId, data.offer_id),
          eq(d.offerSnapshots.protocolId, data.protocol_id),
        ),
      );

    console.log("Removed offer", data);
  }

  private async _completionImpl(
    req: Request,
    res: Response,
    requestBodySchema:
      | typeof openai.completions.RequestBodySchema
      | typeof openai.chatCompletions.RequestBodySchema,
    chunkSchema:
      | typeof openai.completions.CompletionChunkSchema
      | typeof openai.chatCompletions.CompletionChunkSchema,
    responseSchema:
      | typeof openai.completions.ResponseSchema
      | typeof openai.chatCompletions.ResponseSchema,
  ): Promise<void> {
    const parseResult = v.safeParse(requestBodySchema, req.body);

    if (!parseResult.success) {
      const error = v.flatten(parseResult.issues);
      res.status(400).json({ error });
      return;
    }

    const body = parseResult.output;
    console.error(body);

    const configModel = this.config.server.models[body.model];

    if (!configModel) {
      res.status(404).json({ error: "Model not configured" });
      return;
    }

    const offerSnapshots = (
      await d.db
        .select({
          id: d.offerSnapshots.id,
          inputTokenPricePol: d.offerSnapshots.inputTokenPricePol,
          outputTokenPricePol: d.offerSnapshots.outputTokenPricePol,
          payload: d.offerSnapshots.protocolPayload,
        })
        .from(d.offerSnapshots)
        .where(
          and(
            eq(d.offerSnapshots.modelId, body.model),
            gte(d.offerSnapshots.contextSize, configModel.minContextSize),
            lte(
              d.offerSnapshots.inputTokenPricePol,
              parseFloat(configModel.maxInputPrice.$pol),
            ),
            lte(
              d.offerSnapshots.outputTokenPricePol,
              parseFloat(configModel.maxOutputPrice.$pol),
            ),
          ),
        )
    )
      // Sort by `avg(inputTokenPricePol, outputTokenPricePol)`.
      .sort(
        (a, b) =>
          (b.inputTokenPricePol + b.outputTokenPricePol) / 2 -
          (a.inputTokenPricePol + a.outputTokenPricePol) / 2,
      );

    if (offerSnapshots.length === 0) {
      res.status(404).json({ error: "No matching offers" });
      return;
    }

    offerSnapshotLoop: for (const offerSnapshot of offerSnapshots) {
      console.debug("offerSnapshotLoop", offerSnapshot);

      const jobResult = await safeTryAsync(
        this._rpc.consumerCreateJob({
          offer_snapshot_id: offerSnapshot.id,
          currency: Currency.Polygon,
          job_args: JSON.stringify(body),
        }),
      );

      if (!jobResult.success) {
        if (jobResult.error instanceof ConsumerCreateJobError) {
          switch (jobResult.error.data.tag) {
            case "LocalOfferNotFound":
              // Backend inconsistency.
              throw jobResult.error;

            case "InvalidJobArgs":
            case "ProviderInvalidResponse":
              // Provider violates the protocol.
              // TODO: Ban provider locally.
              console.warn(jobResult.error);
              continue offerSnapshotLoop;

            case "ProviderBusy":
            case "ProviderOfferNotFound":
            case "ProviderOfferPayloadMismatch":
            case "ProviderUnreacheable":
              // Normal response, should try again later.
              console.debug(jobResult.error.data.tag);
              continue offerSnapshotLoop;

            default:
              throw unreachable(jobResult.error.data);
          }
        } else {
          throw jobResult.error;
        }
      }

      const job = jobResult.output;
      console.debug(job);
      const { provider_peer_id, provider_job_id } = job;

      const jobConnectionResult = await safeTryAsync(
        pRetry(() => this._rpc.consumerOpenJobConnection(job), {
          shouldRetry: async (e) => {
            if (e instanceof ConsumerOpenJobConnectionError) {
              switch (e.data.tag) {
                case "LocalJobNotFound":
                case "OtherLocalError":
                  // Backend inconsistency.
                  return false;

                case "OtherRemoteError":
                case "ProviderJobExpired":
                case "ProviderJobNotFound":
                  // TODO: Ban provider locally.
                  //

                  console.warn(e);

                  await this._rpc.failJob({
                    provider_job_id,
                    provider_peer_id,
                    reason: `Could not open job connection: ${e.data.tag}`,
                    reason_class: openai.ReasonClass.ProtocolViolation,
                    private_payload: JSON.stringify({
                      request: body,
                    }),
                  });

                  return false;

                case "ProviderBusy":
                case "ProviderUnreacheable":
                  // Should try again.
                  return true;

                default:
                  throw unreachable(e.data);
              }
            } else {
              throw e;
            }
          },
          retries: 2,
        }),
      );

      if (!jobConnectionResult.success) {
        if (
          jobConnectionResult.error instanceof ConsumerOpenJobConnectionError
        ) {
          switch (jobConnectionResult.error.data.tag) {
            case "LocalJobNotFound":
            case "OtherLocalError":
              throw jobConnectionResult.error;

            case "OtherRemoteError":
            case "ProviderJobExpired":
            case "ProviderJobNotFound":
            case "ProviderBusy":
            case "ProviderUnreacheable":
              continue offerSnapshotLoop;

            default:
              throw unreachable(jobConnectionResult.error.data);
          }
        } else {
          throw jobConnectionResult.error;
        }
      }

      const jobConnection = jobConnectionResult.output;
      const { stream } = jobConnection;

      /**
       * Wrapper which reports job failure on timeout.
       */
      const withConnectionTimeout = async <T>(
        awaitable: PromiseLike<T>,
        milliseconds: number,
        privatePayload: () => string,
        consoleMessage = "Timeout while waiting for response",
        reason = "Timeout",
      ): Promise<T | TimeoutError> => {
        const result = await safeTryAsync(
          pTimeout(awaitable, { milliseconds }),
        );

        if (!result.success) {
          if (result.error instanceof TimeoutError) {
            console.warn(consoleMessage);

            await this._rpc.failJob({
              provider_job_id,
              provider_peer_id,
              reason,
              reason_class: openai.ReasonClass.ServiceError,
              private_payload: privatePayload(),
            });

            return result.error;
          } else {
            throw result.error;
          }
        }

        return result.output;
      };

      console.debug("Waiting for prologue...");

      const prologue = await withConnectionTimeout(
        readCborOnce<openai.ResponsePrologue>(stream),
        toMilliseconds({ seconds: 30 }),
        () => JSON.stringify({ request: body }),
      );

      if (prologue instanceof TimeoutError) {
        console.debug("Skipping offer snapshot...");
        continue offerSnapshotLoop;
      }

      console.debug("Read prologue", prologue);

      if (!prologue) {
        console.warn(`Could not read prologue`);

        await this._rpc.failJob({
          provider_job_id,
          provider_peer_id,
          reason: "Could not read prologue",
          reason_class: openai.ReasonClass.ServiceError,
          private_payload: JSON.stringify({
            request: body,
          }),
        });

        continue offerSnapshotLoop;
      }

      switch (prologue.status) {
        case "Ok":
          console.debug("Prologue Ok");
          break;

        case "ServiceError":
          console.log(
            `Provider responded with service error`,
            prologue.message,
          );

          await this._rpc.failJob({
            provider_job_id,
            provider_peer_id,
            reason: prologue.message ?? "Service error",
            reason_class: openai.ReasonClass.ServiceError,
          });

          continue offerSnapshotLoop;

        default:
          throw unreachable(prologue);
      }

      if (body.stream) {
        // By protocol, provider sends us CBOR objects.
        // For the end-user, we're replicating OpenAI server-sent events.
        //

        res.writeHead(200, {
          "Content-Type": "text/event-stream",
          Connection: "keep-alive",
          "Cache-Control": "no-cache",
        });

        const chunks: (
          | openai.completions.CompletionChunk
          | openai.chatCompletions.CompletionChunk
        )[] = [];

        let usage;

        while (true) {
          // BUG: Shall properly handle timeout.
          console.debug("Waiting for stream chunk...");

          const streamChunk = await readCborOnce<
            | openai.completions.CompletionChunk
            | openai.chatCompletions.CompletionChunk
            | openai.StreamingEpilogueChunk
          >(stream);

          console.debug("streamChunk", streamChunk);

          switch (streamChunk?.object) {
            case "chat.completion.chunk":
            case "text_completion": {
              const openaiChunk = v.safeParse(chunkSchema, streamChunk);

              if (!openaiChunk.success) {
                console.warn(
                  "[Protocol] Invalid OpenAI stream chunk",
                  v.flatten(openaiChunk.issues),
                );

                await this._rpc.failJob({
                  provider_peer_id,
                  provider_job_id,
                  reason: `Invalid OpenAI stream chunk: ${JSON.stringify(
                    v.flatten(openaiChunk.issues),
                  )}`,
                  reason_class: openai.ReasonClass.ProtocolViolation,
                });

                res.write(`data: [DONE]\n\n`);
                res.end();

                return;
              }

              if (openaiChunk.output.usage) {
                usage = openaiChunk.output.usage;
              }

              chunks.push(openaiChunk.output);

              res.write(`data: ${JSON.stringify(openaiChunk.output)}\n\n`);

              break;
            }

            case "derouter.epilogue": {
              // Check usage.
              //

              res.write(`data: [DONE]\n\n`);
              res.end();

              if (!usage) {
                console.warn("[Protocol] Missing usage in the response");

                await this._rpc.failJob({
                  provider_peer_id,
                  provider_job_id,
                  reason: `Usage is missing`,
                  reason_class: openai.ReasonClass.ProtocolViolation,
                  private_payload: JSON.stringify({
                    request: body,
                    response: chunks,
                  }),
                });

                return;
              }

              // Check balance delta.
              //

              const balanceDelta = openai.calcCost(
                offerSnapshot.payload,
                usage,
              );

              if (balanceDelta !== streamChunk.balance_delta) {
                console.warn("[Protocol] Balance delta mismatch", {
                  ours: balanceDelta,
                  their: streamChunk.balance_delta,
                });

                await this._rpc.failJob({
                  provider_peer_id,
                  provider_job_id,
                  reason: `Balance delta mismatch (ours: ${
                    balanceDelta
                  }, their: ${streamChunk.balance_delta})`,
                  reason_class: openai.ReasonClass.ProtocolViolation,
                  private_payload: JSON.stringify({
                    request: body,
                    response: chunks,
                  }),
                });

                return;
              }

              // Check public payload.
              //

              const publicPayloadError =
                "prompt" in body
                  ? validatateCompletionsPublicPayload(
                      streamChunk.public_payload,
                      body,
                      usage,
                    )
                  : validatateChatCompletionsPublicPayload(
                      streamChunk.public_payload,
                      body,
                      usage,
                    );

              if (publicPayloadError) {
                console.warn(
                  `[Protocol] Public payload validation failed: ${
                    publicPayloadError
                  }`,
                );

                await this._rpc.failJob({
                  provider_peer_id,
                  provider_job_id,
                  reason: `Invalid public payload (${publicPayloadError})`,
                  reason_class: openai.ReasonClass.ProtocolViolation,
                  private_payload: JSON.stringify({
                    request: body,
                    response: chunks,
                  }),
                });

                return;
              }

              // Success!
              //

              await this._rpc.consumerCompleteJob({
                provider_peer_id,
                provider_job_id,
                balance_delta: streamChunk.balance_delta,
                public_payload: streamChunk.public_payload,
                completed_at_sync: streamChunk.completed_at_sync,
                private_payload: JSON.stringify({
                  request: body,
                  response: chunks,
                }),
              });

              console.log(
                `✅ Received completion (${
                  streamChunk.balance_delta
                    ? `$POL ~${parseWeiToEth(streamChunk.balance_delta)}`
                    : "free"
                })`,
              );

              return;
            }

            case undefined:
              console.warn("Provider closed stream prematurely");

              await this._rpc.failJob({
                provider_peer_id,
                provider_job_id,
                reason: `Provider closed stream prematurely`,
                reason_class: openai.ReasonClass.ServiceError,
                private_payload: JSON.stringify({
                  request: body,
                  response: chunks,
                }),
              });

              return;

            default:
              throw unreachable(streamChunk);
          }
        }
      } else {
        console.debug("Waiting for response...");
        const responseCbor = await withConnectionTimeout(
          readCborOnce(stream),

          // TODO: Make it configurable?
          toMilliseconds({ seconds: 300 }),

          () => JSON.stringify({ request: body }),
        );

        if (responseCbor instanceof TimeoutError) {
          continue offerSnapshotLoop;
        }

        if (responseCbor === undefined) {
          console.warn("Provider did not respond");

          await this._rpc.failJob({
            provider_peer_id,
            provider_job_id,
            reason: `Provider did not respond`,
            reason_class: openai.ReasonClass.ServiceError,
            private_payload: JSON.stringify({
              request: body,
            }),
          });

          continue offerSnapshotLoop;
        }

        const completion = v.safeParse(responseSchema, responseCbor);

        if (!completion.success) {
          console.warn(
            "Provider sent invalid completion object",
            v.flatten(completion.issues),
          );

          await this._rpc.failJob({
            provider_peer_id,
            provider_job_id,
            reason: `Provider sent invalid completion object: ${JSON.stringify(
              v.flatten(completion.issues),
            )}`,
            reason_class: openai.ReasonClass.ProtocolViolation,
            private_payload: JSON.stringify({
              request: body,
            }),
          });

          continue offerSnapshotLoop;
        }

        console.debug(completion.output);
        res.status(201).json(completion.output);

        // Check usage.
        //

        if (!completion.output.usage) {
          console.warn("[Protocol] Missing usage in the response");

          await this._rpc.failJob({
            provider_peer_id,
            provider_job_id,
            reason: `Usage is missing`,
            reason_class: openai.ReasonClass.ProtocolViolation,
            private_payload: JSON.stringify({
              request: body,
              response: completion.output,
            }),
          });

          return;
        }

        // Read epilogue.
        //

        const epilogue =
          await readCborOnce<openai.NonStreamingResponseEpilogue>(stream);

        if (!epilogue) {
          console.warn("[Protocol] Missing epilogue");

          await this._rpc.failJob({
            provider_peer_id,
            provider_job_id,
            reason: `Epilogue is missing`,
            reason_class: openai.ReasonClass.ProtocolViolation,
            private_payload: JSON.stringify({
              request: body,
              response: completion.output,
            }),
          });

          return;
        }

        // Check balance delta.
        //

        const balanceDelta = openai.calcCost(
          offerSnapshot.payload,
          completion.output.usage,
        );

        if (balanceDelta !== epilogue.balance_delta) {
          console.warn("[Protocol] Balance delta mismatch", {
            ours: balanceDelta,
            their: epilogue.balance_delta,
          });

          await this._rpc.failJob({
            provider_peer_id,
            provider_job_id,
            reason: `Balance delta mismatch (ours: ${
              balanceDelta
            }, their: ${epilogue.balance_delta})`,
            reason_class: openai.ReasonClass.ProtocolViolation,
            private_payload: JSON.stringify({
              request: body,
              response: completion.output,
            }),
          });

          return;
        }

        // Check public payload.
        //

        const publicPayloadError =
          "prompt" in body
            ? validatateCompletionsPublicPayload(
                epilogue.public_payload,
                body,
                completion.output.usage,
              )
            : validatateChatCompletionsPublicPayload(
                epilogue.public_payload,
                body,
                completion.output.usage,
              );

        if (publicPayloadError) {
          console.warn(
            `[Protocol] Public payload validation failed: ${
              publicPayloadError
            }`,
          );

          await this._rpc.failJob({
            provider_peer_id,
            provider_job_id,
            reason: `Invalid public payload (${publicPayloadError})`,
            reason_class: openai.ReasonClass.ProtocolViolation,
            private_payload: JSON.stringify({
              request: body,
              response: completion.output,
            }),
          });

          return;
        }

        // Success!
        //

        await this._rpc.consumerCompleteJob({
          provider_peer_id,
          provider_job_id,
          balance_delta: epilogue.balance_delta,
          public_payload: epilogue.public_payload,
          completed_at_sync: epilogue.completed_at_sync,
          private_payload: JSON.stringify({
            request: body,
            response: completion.output,
          }),
        });

        console.log(
          `✅ Received completion (${
            epilogue.balance_delta
              ? `$POL ~${parseWeiToEth(epilogue.balance_delta)}`
              : "free"
          })`,
        );

        return;
      }
    }

    res.status(503).json({ error: "Could not complete the request" });
  }
}

function validatateCompletionsPublicPayload(
  publicPayloadString: string,
  body: openai.completions.RequestBody,
  usage: openai.Usage,
): string | null {
  let json;

  try {
    json = JSON.parse(publicPayloadString);
  } catch (e: any) {
    console.warn(
      "[Protocol] Failed to parse public payload JSON string",
      e.message,
    );

    return `JSON parsing failed: ${e.message}`;
  }

  const parseResult = v.safeParse(
    openai.completions.PublicJobPayloadSchema,
    json,
  );

  if (!parseResult.success) {
    console.warn(
      "[Protocol] Failed to parse public payload object",
      v.flatten(parseResult.issues),
    );

    return `Payload parsing failed: ${v.flatten(parseResult.issues)}`;
  }

  const publicPayload = parseResult.output;

  if (publicPayload.request.frequency_penalty !== body.frequency_penalty) {
    return `Payload mismatch: request.frequency_penalty (${publicPayload.request.frequency_penalty})`;
  } else if (publicPayload.request.max_tokens !== body.max_tokens) {
    return `Payload mismatch: request.max_tokens (${publicPayload.request.max_tokens})`;
  } else if (publicPayload.request.model !== body.model) {
    return `Payload mismatch: request.model (${publicPayload.request.max_tokens})`;
  } else if (publicPayload.request.n !== body.n) {
    return `Payload mismatch: request.n (${publicPayload.request.n})`;
  } else if (publicPayload.request.presence_penalty !== body.presence_penalty) {
    return `Payload mismatch: request.presence_penalty (${publicPayload.request.presence_penalty})`;
  } else if (publicPayload.request.stream !== body.stream) {
    return `Payload mismatch: request.stream (${publicPayload.request.stream})`;
  } else if (publicPayload.request.temperature !== body.temperature) {
    return `Payload mismatch: request.temperature (${publicPayload.request.temperature})`;
  } else if (publicPayload.request.top_p !== body.top_p) {
    return `Payload mismatch: request.top_p (${publicPayload.request.top_p})`;
  }

  if (!deepEqual(publicPayload.response.usage, usage)) {
    return `Payload mismatch: response.usage (${publicPayload.response.usage})`;
  }

  return null;
}

function validatateChatCompletionsPublicPayload(
  publicPayloadString: string,
  body: openai.chatCompletions.RequestBody,
  usage: openai.Usage,
): string | null {
  let json;

  try {
    json = JSON.parse(publicPayloadString);
  } catch (e: any) {
    console.warn(
      "[Protocol] Failed to parse public payload JSON string",
      e.message,
    );

    return `JSON parsing failed: ${e.message}`;
  }

  const parseResult = v.safeParse(
    openai.chatCompletions.PublicJobPayloadSchema,
    json,
  );

  if (!parseResult.success) {
    console.warn(
      "[Protocol] Failed to parse public payload object",
      v.flatten(parseResult.issues),
    );

    return `Payload parsing failed: ${v.flatten(parseResult.issues)}`;
  }

  const publicPayload = parseResult.output;

  if (publicPayload.request.frequency_penalty !== body.frequency_penalty) {
    return `Payload mismatch: request.frequency_penalty (${publicPayload.request.frequency_penalty})`;
  } else if (publicPayload.request.max_tokens !== body.max_tokens) {
    return `Payload mismatch: request.max_tokens (${publicPayload.request.max_tokens})`;
  } else if (publicPayload.request.model !== body.model) {
    return `Payload mismatch: request.model (${publicPayload.request.max_tokens})`;
  } else if (publicPayload.request.n !== body.n) {
    return `Payload mismatch: request.n (${publicPayload.request.n})`;
  } else if (publicPayload.request.presence_penalty !== body.presence_penalty) {
    return `Payload mismatch: request.presence_penalty (${publicPayload.request.presence_penalty})`;
  } else if (publicPayload.request.stream !== body.stream) {
    return `Payload mismatch: request.stream (${publicPayload.request.stream})`;
  } else if (publicPayload.request.temperature !== body.temperature) {
    return `Payload mismatch: request.temperature (${publicPayload.request.temperature})`;
  } else if (publicPayload.request.top_p !== body.top_p) {
    return `Payload mismatch: request.top_p (${publicPayload.request.top_p})`;
  } else if (
    publicPayload.request.max_completion_tokens !== body.max_completion_tokens
  ) {
    return `Payload mismatch: request.max_completion_tokens (${publicPayload.request.max_completion_tokens})`;
  } else if (publicPayload.request.reasoning_effort !== body.reasoning_effort) {
    return `Payload mismatch: request.reasoning_effort (${publicPayload.request.reasoning_effort})`;
  } else if (publicPayload.request.response_format !== body.response_format) {
    return `Payload mismatch: request.response_format (${publicPayload.request.response_format})`;
  }

  if (!deepEqual(publicPayload.response.usage, usage)) {
    return `Payload mismatch: response.usage`;
  }

  return null;
}
