import * as openai from "@derouter/protocol-openai";
import {
  ConsumerOpenConnectionError,
  RPC,
  type JobRecord,
  type OfferRemoved,
  type OfferSnapshot,
  type ProviderHeartbeat,
  type ProviderRecord,
} from "@derouter/rpc";
import { readCborOnce, unreachable, writeCbor } from "@derouter/rpc/util";
import bodyParser from "body-parser";
import { and, eq, gt, gte, lte, or, sql } from "drizzle-orm";
import express, { type Request, type Response } from "express";
import deepEqual from "fast-deep-equal";
import json5 from "json5";
import * as fs from "node:fs";
import { Duplex } from "node:stream";
import { parseArgs } from "node:util";
import * as v from "valibot";
import { d, dbMigrated } from "./lib/drizzle.js";
import { parseWeiToEth } from "./lib/util.js";

enum FailureReason {
  ProtocolViolation,
  ServiceError,
}

enum Currency {
  Polygon,
}

const PriceSchema = v.object({
  $pol: v.pipe(
    v.string(),
    v.check((x) => {
      let num = parseFloat(x);
      if (Number.isNaN(num)) return false;
      if (num < 0) return false;
      return true;
    }, "Must be parsed as a positive number"),
  ),
});

export const ConfigSchema = v.object({
  server: v.object({
    host: v.optional(v.string(), "localhost"),

    port: v.pipe(
      v.number(),
      v.integer(),
      v.minValue(0),
      v.maxValue(2 ** 16 - 1),
    ),

    models: v.record(
      v.string(),
      v.object({
        minContextSize: v.pipe(v.number(), v.integer(), v.minValue(1)),
        maxInputPrice: PriceSchema,
        maxOutputPrice: PriceSchema,
        minTrialAllowance: v.optional(PriceSchema),
      }),
    ),
  }),

  rpc: v.optional(
    v.object({
      host: v.optional(v.string(), "127.0.0.1"),
      port: v.optional(v.number(), 4269),
    }),
    {
      host: "127.0.0.1",
      port: 4269,
    },
  ),

  autoDeposit: v.optional(
    v.record(
      v.string(),
      v.object({
        treshold: v.string(),
        amount: v.string(),
      }),
    ),
  ),
});

const { values } = parseArgs({
  args: process.argv,
  allowPositionals: true,
  options: {
    config: {
      type: "string",
      short: "c",
    },
  },
});

const configPath = values.config;

if (!configPath) {
  console.error("--config or -c argument expected");
  process.exit(1);
}

const configText = fs.readFileSync(configPath, { encoding: "utf8" });
const configJson = json5.parse(configText);
const config = v.parse(ConfigSchema, configJson);
console.dir(config, { depth: null, colors: true });

type Connection = {
  connectionId: number;
  stream: Duplex;
};

class OpenAiConsumer {
  private _connectionPools = new Map<number, Connection[]>();
  private _rpc: RPC;

  constructor(config: v.InferOutput<typeof ConfigSchema>) {
    this._rpc = new RPC(config.rpc.host, config.rpc.port);

    this._rpc.emitter.on("providerUpdated", (e) => this.onProviderUpdated(e));
    this._rpc.emitter.on("providerHeartbeat", (e) =>
      this.onProviderHeartbeat(e),
    );
    this._rpc.emitter.on("offerUpdated", (e) => this.onOfferUpdated(e));
    this._rpc.emitter.on("offerRemoved", (e) => this.onOfferRemoved(e));
    this._rpc.emitter.on("jobUpdated", (e) => this.onJobUpdated(e));

    this.sync().then(() => this.run());
  }

  private async sync() {
    const { peer_id: peerId } = await this._rpc.querySystem();

    const allJobs: Awaited<ReturnType<typeof this._rpc.queryJobs>> = [];
    const providerPeerIds = new Set<string>();
    const offerSnapshotIds = new Set<number>();

    let queriedJobs;
    do {
      queriedJobs = await this._rpc.queryJobs({
        database_row_id_cursor: queriedJobs?.at(-1)!.job_rowid,
        consumer_peer_ids: [peerId],
        protocol_ids: [openai.ProtocolId],
        limit: 100,
      });

      queriedJobs
        .map((j) => j.provider_peer_id)
        .forEach((id) => providerPeerIds.add(id));

      queriedJobs
        .map((j) => j.offer_snapshot_rowid)
        .forEach((id) => offerSnapshotIds.add(id));

      allJobs.push(...queriedJobs);
    } while (queriedJobs.length);

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

        const parseResult = v.safeParse(
          openai.OfferPayloadSchema,
          offer.protocol_payload,
        );

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

      for (const job of allJobs) {
        if (failedOfferSnapshotIds.has(job.offer_snapshot_rowid)) {
          console.warn(
            `Skipping job #${
              job.job_rowid
            } due to failed offer snapshot insertion`,
          );

          continue;
        }

        console.debug("Inserting", job);

        console.debug(
          "Inserted",
          (
            await tx
              .insert(d.jobs)
              .values({
                id: job.job_rowid,
                offerSnapshotId: job.offer_snapshot_rowid,
                currency: job.currency,
                balanceDelta: job.balance_delta,
                publicPayload: job.public_payload,
                privatePayload: job.private_payload,
                reason: job.reason,
                reasonClass: job.reason_class,
                createdAtLocal: job.created_at_local,
                createdAtSync: job.created_at_sync,
                completedAtLocal: job.completed_at_local,
                completedAtSync: job.completed_at_sync,
                signatureConfirmedAtLocal: job.signature_confirmed_at_local,
                confirmationError: job.confirmation_error,
              })
              .returning()
          )[0],
        );
      }
    });

    this._rpc.subscribeToActiveOffers({ protocol_ids: [openai.ProtocolId] });
    this._rpc.subscribeToActiveProviders();

    this._rpc.subscribeToJobs({
      protocol_ids: [openai.ProtocolId],
      consumer_peer_ids: [peerId],
    });
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

    app.listen(config.server.port, config.server.host, () => {
      console.log(
        `Server listening on http://${config.server.host}:${config.server.port}`,
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

      const parseResult = v.safeParse(
        openai.OfferPayloadSchema,
        data.protocol_payload,
      );

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

  async onJobUpdated(data: JobRecord) {
    console.debug("onJobUpdated", data);
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

    const configModel = config.server.models[body.model];

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
          activeServiceConnectionsCount: sql<number>`
            cast(count(${d.activeServiceConnections.id}) AS INT)
          `,
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
        .leftJoin(
          d.activeServiceConnections,
          eq(d.offerSnapshots.id, d.activeServiceConnections.offerSnapshotId),
        )
        .groupBy(d.offerSnapshots.id)
        .having(({ activeServiceConnectionsCount }) =>
          or(
            eq(d.offerSnapshots.active, true),
            gt(activeServiceConnectionsCount, 0),
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
      let connection = this._connectionPools.get(offerSnapshot.id)?.shift();

      if (connection) {
        console.debug(`Reusing connection for ${offerSnapshot.id}`);
      } else {
        console.debug(`Opening new connection for ${offerSnapshot.id}...`);

        try {
          // BUG: Shall handle timeout.
          const result = await this._rpc.consumerOpenConnection({
            offer_snapshot_id: offerSnapshot.id,
            currency: Currency.Polygon,
          });

          connection = {
            connectionId: result.connectionId,
            stream: result.stream,
          };

          console.debug(`Successfully opened new connection`, {
            connectionId: connection.connectionId,
          });
        } catch (e) {
          // BUG: Shall save the error for future reference
          // (e.g. to block the provider).
          if (e instanceof ConsumerOpenConnectionError) {
            console.error(e.message);
            continue offerSnapshotLoop;
          } else {
            throw e;
          }
        }
      }

      try {
        const { database_job_id } = await this._rpc.consumerCreateJob({
          connection_id: connection.connectionId,
          private_payload: JSON.stringify({ request: body }),
        });

        console.debug("Writing body to the stream...");
        await writeCbor(connection.stream, body);

        // BUG: Shall handle timeout.
        console.debug("Waiting for prologue...");
        const prologue = await readCborOnce<openai.ResponsePrologue>(
          connection.stream,
        );
        console.debug("Read prologue", prologue);

        if (!prologue) {
          // TODO: Report service error.
          // NOTE: We don't even have a job ID at this point...
          console.warn(`Could not read prologue`);
          continue offerSnapshotLoop;
        }

        switch (prologue.status) {
          case "Ok":
            console.debug("Prologue Ok");
            break;

          case "ProtocolViolation": {
            // They accuse us of protocol violation, but we're honest.
            //

            console.warn(
              `Provider accused us of protocol violation`,
              prologue.message,
            );

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `They accused us of protocol violation: ${prologue.message}`,
              reason_class: FailureReason.ProtocolViolation,
            });

            continue offerSnapshotLoop;
          }

          case "ServiceError":
            console.log(
              `Provider responded with service error`,
              prologue.message,
            );

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: prologue.message ?? "Service error",
              reason_class: FailureReason.ServiceError,
            });

            continue offerSnapshotLoop;

          default:
            throw unreachable(prologue);
        }

        console.debug("this._rpc.consumerSyncJob()...", {
          database_job_id,
          provider_job_id: prologue.provider_job_id,
          created_at_sync: prologue.created_at_sync,
        });

        await this._rpc.consumerSyncJob({
          database_job_id,
          provider_job_id: prologue.provider_job_id,
          created_at_sync: prologue.created_at_sync,
        });

        if (body.stream) {
          // By protocol, provider sends us CBOR objects.
          // For the end-user, we're replicating OpenAI server-sent events.
          //

          res.header("Content-Type", "text/event-stream");

          const chunks: (
            | openai.completions.CompletionChunk
            | openai.chatCompletions.CompletionChunk
          )[] = [];
          let usage;

          while (true) {
            // BUG: Shall handle timeout.
            console.debug("Waiting for stream chunk...");

            const streamChunk = await readCborOnce<
              openai.completions.Chunk | openai.chatCompletions.Chunk
            >(connection.stream);

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

                  await this._rpc.consumerFailJob({
                    database_job_id,
                    reason: `Invalid OpenAI stream chunk: ${JSON.stringify(
                      v.flatten(openaiChunk.issues),
                    )}`,
                    reason_class: FailureReason.ProtocolViolation,
                  });

                  res.write(`event:\ndata: [DONE]\n\n`);
                  res.end();

                  return;
                }

                if (openaiChunk.output.usage) {
                  usage = openaiChunk.output.usage;
                }

                chunks.push(openaiChunk.output);

                res.write(
                  `event:\ndata: ${JSON.stringify(openaiChunk.output)}\n\n`,
                );

                break;
              }

              case "derouter.epilogue": {
                // Check usage.
                //

                res.write(`event:\ndata: [DONE]\n\n`);
                res.end();

                if (!usage) {
                  console.warn("[Protocol] Missing usage in the response");

                  await this._rpc.consumerFailJob({
                    database_job_id,
                    reason: `Usage is missing`,
                    reason_class: FailureReason.ProtocolViolation,
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

                  await this._rpc.consumerFailJob({
                    database_job_id,
                    reason: `Balance delta mismatch (ours: ${
                      balanceDelta
                    }, their: ${streamChunk.balance_delta})`,
                    reason_class: FailureReason.ProtocolViolation,
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

                  await this._rpc.consumerFailJob({
                    database_job_id,
                    reason: `Invalid public payload (${publicPayloadError})`,
                    reason_class: FailureReason.ProtocolViolation,
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
                  balance_delta: streamChunk.balance_delta,
                  database_job_id,
                  public_payload: streamChunk.public_payload,
                  completed_at_sync: streamChunk.completed_at_sync,
                  private_payload: JSON.stringify({
                    request: body,
                    response: chunks,
                  }),
                });

                // NOTE: It may take long time.
                // TODO: Allow confirming directly via current connection.
                // BUG: Catch `ProviderUnreacheable` error.
                await this._rpc.consumerConfirmJobCompletion({
                  database_job_id,
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

                await this._rpc.consumerFailJob({
                  database_job_id,
                  reason: `Provider closed stream prematurely`,
                  reason_class: FailureReason.ServiceError,
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
          // TODO: Timeout.
          console.debug("Waiting for response...");
          const responseCbor = await readCborOnce(connection.stream);

          if (responseCbor === undefined) {
            console.warn("Provider did not respond");

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `Provider did not respond`,
              reason_class: FailureReason.ServiceError,
            });

            continue offerSnapshotLoop;
          }

          const completion = v.safeParse(responseSchema, responseCbor);

          if (!completion.success) {
            console.warn(
              "Provider sent invalid completion object",
              v.flatten(completion.issues),
            );

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `Provider sent invalid completion object: ${JSON.stringify(
                v.flatten(completion.issues),
              )}`,
              reason_class: FailureReason.ProtocolViolation,
            });

            continue offerSnapshotLoop;
          }

          console.debug(completion.output);
          res.status(201).json(completion.output);

          // Check usage.
          //

          if (!completion.output.usage) {
            console.warn("[Protocol] Missing usage in the response");

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `Usage is missing`,
              reason_class: FailureReason.ProtocolViolation,
              private_payload: JSON.stringify({
                request: body,
                response: completion.output,
              }),
            });

            return;
          }

          const epilogue = await readCborOnce<
            openai.completions.Epilogue | openai.chatCompletions.Epilogue
          >(connection.stream);

          if (!epilogue) {
            console.warn("[Protocol] Missing epilogue");

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `Epilogue is missing`,
              reason_class: FailureReason.ProtocolViolation,
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

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `Balance delta mismatch (ours: ${
                balanceDelta
              }, their: ${epilogue.balance_delta})`,
              reason_class: FailureReason.ProtocolViolation,
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

            await this._rpc.consumerFailJob({
              database_job_id,
              reason: `Invalid public payload (${publicPayloadError})`,
              reason_class: FailureReason.ProtocolViolation,
              private_payload: JSON.stringify({
                request: body,
                response: completion.output,
              }),
            });

            return;
          }

          await this._rpc.consumerCompleteJob({
            database_job_id,
            balance_delta: epilogue.balance_delta,
            public_payload: epilogue.public_payload,
            completed_at_sync: epilogue.completed_at_sync,
            private_payload: JSON.stringify({
              request: body,
              response: completion.output,
            }),
          });

          // NOTE: It may take long time.
          // TODO: Allow confirming directly via current connection.
          // BUG: Catch `ProviderUnreacheable` error.
          await this._rpc.consumerConfirmJobCompletion({
            database_job_id,
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
      } catch (e) {
        console.error(e);
        throw e;
      } finally {
        let pool = this._connectionPools.get(offerSnapshot.id);

        if (!pool) {
          pool = [];
          this._connectionPools.set(offerSnapshot.id, pool);
        }

        pool.push(connection);
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

dbMigrated.promise.then(() => {
  new OpenAiConsumer(config);
});
