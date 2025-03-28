import {
  Consumer,
  OfferRemovedData,
  OfferUpdatedData,
  OpenConnectionError,
  ProviderHeartbeatData,
  ProviderUpdatedData,
} from "@derouter/consumer";
import { readCborOnce, unreachable, writeCbor } from "@derouter/consumer/util";
import * as openaiProtocol from "@derouter/protocol-openai";
import bodyParser from "body-parser";
import { and, eq, gt, gte, lte, or, sql } from "drizzle-orm";
import express, { Request, Response } from "express";
import json5 from "json5";
import * as fs from "node:fs";
import { Duplex } from "node:stream";
import { parseArgs } from "node:util";
import * as v from "valibot";
import { d } from "./lib/drizzle.js";
import { parseWeiToEth } from "./lib/util.js";

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

class OpenAiConsumer extends Consumer {
  private _connectionPools = new Map<number, Connection[]>();

  async run() {
    const app = express();

    app.get("/", (req, res) => {
      res.sendStatus(200);
    });

    app.post("/v1/completions", bodyParser.json(), (req, res) => {
      this._completionImpl(
        req,
        res,
        openaiProtocol.completions.RequestBodySchema,
        openaiProtocol.completions.ChunkSchema,
        openaiProtocol.completions.ResponseSchema,
      );
    });

    app.post("/v1/chat/completions", bodyParser.json(), (req, res) => {
      this._completionImpl(
        req,
        res,
        openaiProtocol.chatCompletions.RequestBodySchema,
        openaiProtocol.chatCompletions.ChunkSchema,
        openaiProtocol.chatCompletions.ResponseSchema,
      );
    });

    app.listen(config.server.port, config.server.host, () => {
      console.log(
        `Server listening on http://${config.server.host}:${config.server.port}`,
      );
    });

    await this.loop();
  }

  async onProviderUpdated(data: ProviderUpdatedData) {
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

  async onProviderHeartbeat(data: ProviderHeartbeatData) {
    await d.db
      .update(d.providers)
      .set({ latestHeartbeatAt: data.latest_heartbeat_at })
      .where(eq(d.providers.peerId, data.peer_id));

    console.debug("Provider heartbeat", data);
  }

  async onOfferUpdated(data: OfferUpdatedData) {
    console.debug("onOfferUpdated", data);

    await d.db.transaction(async (tx) => {
      if (data.protocol_id !== openaiProtocol.ProtocolId) {
        console.debug("Skipped offer with protocol", data.protocol_id);
        return;
      }

      const parseResult = v.safeParse(
        openaiProtocol.OfferPayloadSchema,
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

  async onOfferRemoved(data: OfferRemovedData) {
    if (data.protocol_id !== openaiProtocol.ProtocolId) {
      console.debug("Skipped offer with protocol", data.protocol_id);
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
      | typeof openaiProtocol.completions.RequestBodySchema
      | typeof openaiProtocol.chatCompletions.RequestBodySchema,
    chunkSchema:
      | typeof openaiProtocol.completions.ChunkSchema
      | typeof openaiProtocol.chatCompletions.ChunkSchema,
    responseSchema:
      | typeof openaiProtocol.completions.ResponseSchema
      | typeof openaiProtocol.chatCompletions.ResponseSchema,
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
      // FIXME: Reusing connection of a dead provider.
      let connection = this._connectionPools.get(offerSnapshot.id)?.shift();

      if (connection) {
        console.debug(`Reusing connection for ${offerSnapshot.id}`);
      } else {
        console.debug(`Opening new connection for ${offerSnapshot.id}...`);

        try {
          // TODO: Timeout.
          const result = await this.openConnection({
            offer_snapshot_id: offerSnapshot.id,
          });

          connection = {
            connectionId: result.connectionId,
            stream: result.stream,
          };

          console.debug(`Successfully opened new connection`, {
            connectionId: connection.connectionId,
          });
        } catch (e) {
          if (e instanceof OpenConnectionError) {
            console.error(e.message);
            continue offerSnapshotLoop;
          } else {
            throw e;
          }
        }
      }

      try {
        console.debug("Writing body to the stream...");
        await writeCbor(connection.stream, body);

        // TODO: Timeout.
        console.debug("Waiting for prologue...");
        const prologue = await readCborOnce<openaiProtocol.ResponsePrologue>(
          connection.stream,
        );
        console.debug("Read prologue", prologue);

        if (!prologue) {
          // TODO: Report service error.
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

            // TODO: Report protocol error.
            console.warn(
              `Provider accused us of protocol violation`,
              prologue.message,
            );

            continue offerSnapshotLoop;
          }

          case "ServiceError":
            // TODO: Report service error.
            console.log(
              `Provider responded with service error`,
              prologue.message,
            );

            continue offerSnapshotLoop;

          default:
            throw unreachable(prologue);
        }

        if (body.stream) {
          // By protocol, provider sends us CBOR objects.
          // For the end-user, we're replicating OpenAI server-sent events.
          //

          res.header("Content-Type", "text/event-stream");

          while (true) {
            // TODO: Timeout.
            console.debug("Waiting for stream chunk...");

            const streamChunk = await readCborOnce<
              | openaiProtocol.CompletionsStreamChunk
              | openaiProtocol.ChatCompletionsStreamChunk
            >(connection.stream);

            console.debug("streamChunk", streamChunk);

            switch (streamChunk?.object) {
              case "chat.completion.chunk":
              case "text_completion": {
                const openaiChunk = v.safeParse(chunkSchema, streamChunk);

                if (!openaiChunk.success) {
                  // TODO: Report protocol error.
                  console.warn(
                    "Invalid OpenAI stream chunk",
                    v.flatten(openaiChunk.issues),
                  );

                  return;
                }

                res.write(
                  `event:\ndata: ${JSON.stringify(openaiChunk.output)}\n\n`,
                );

                break;
              }

              case "derouter.epilogue":
                console.log(
                  `✅ Received completion (${
                    streamChunk.balanceDelta
                      ? `$POL ~${parseWeiToEth(streamChunk.balanceDelta)}`
                      : "free"
                  })`,
                );

                res.write(`event:\ndata: [DONE]\n\n`);
                res.end();
                return;

              case undefined:
                // TODO: Report service error.
                console.warn("Provider closed stream prematurely");
                res.end();
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
            // TODO: Report service error.
            console.warn("Provider did not respond");
            continue offerSnapshotLoop;
          }

          const completion = v.safeParse(responseSchema, responseCbor);

          if (!completion.success) {
            // TODO: Report protocol error.
            console.warn(
              "Provider sent invalid completion object",
              v.flatten(completion.issues),
            );

            continue offerSnapshotLoop;
          }

          console.log("✅ Received completion", completion.output);

          if (completion.output.usage) {
          } else {
            // TODO: Report protocol error.
            console.warn("Missing usage in response");
          }

          const epilogueCbor =
            await readCborOnce<openaiProtocol.NonStreamingResponseEpilogue>(
              connection.stream,
            );

          if (!epilogueCbor) {
            // TODO: Report protocol error.
            console.warn("Missing usage in response");
          } else {
            console.log(
              `✅ Received completion (${
                epilogueCbor.balanceDelta
                  ? `$POL ~${parseWeiToEth(epilogueCbor.balanceDelta)}`
                  : "free"
              })`,
            );
          }

          res.status(201).json(completion.output);
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

new OpenAiConsumer({}, config.rpc.host, config.rpc.port).run();
