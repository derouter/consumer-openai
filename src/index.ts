import {
  Consumer,
  OfferRemovedData,
  OfferUpdatedData,
  ProviderHeartbeatData,
  ProviderUpdatedData,
} from "@derouter/consumer";
import express from "express";
import json5 from "json5";
import * as fs from "node:fs";
import { parseArgs } from "node:util";
import * as v from "valibot";

const PriceSchema = v.object({
  $pol: v.string(),
});

export const ConfigSchema = v.object({
  server: v.object({
    host: v.optional(v.string(), "localhost"),

    port: v.pipe(
      v.number(),
      v.integer(),
      v.minValue(0),
      v.maxValue(2 ** 16 - 1)
    ),

    models: v.record(
      v.string(),
      v.object({
        minContextSize: v.pipe(v.number(), v.integer(), v.minValue(1)),
        maxInputPrice: PriceSchema,
        maxOutputPrice: PriceSchema,
        minTrialAllowance: v.optional(PriceSchema),
      })
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
    }
  ),

  autoDeposit: v.optional(
    v.record(
      v.string(),
      v.object({
        treshold: v.string(),
        amount: v.string(),
      })
    )
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
  console.error("--config argument expected");
  process.exit(1);
}

const configText = fs.readFileSync(configPath, { encoding: "utf8" });
const configJson = json5.parse(configText);
const config = v.parse(ConfigSchema, configJson);
console.dir(config, { depth: null, colors: true });

class OpenAiConsumer extends Consumer {
  async run() {
    const app = express();

    app.get("/", (req, res) => {
      res.send("Hello World!");
    });

    app.listen(config.server.port, config.server.host, () => {
      console.log(
        `Server listening on http://${config.server.host}:${config.server.port}`
      );
    });

    await this.loop();
  }

  onProviderUpdated(data: ProviderUpdatedData): Promise<void> {
    throw new Error("Method not implemented.");
  }

  onProviderHeartbeat(data: ProviderHeartbeatData): Promise<void> {
    throw new Error("Method not implemented.");
  }

  onOfferUpdated(data: OfferUpdatedData): Promise<void> {
    throw new Error("Method not implemented.");
  }

  onOfferRemoved(data: OfferRemovedData): Promise<void> {
    throw new Error("Method not implemented.");
  }
}

new OpenAiConsumer(
  {
    protocols: ["openai@0"],
  },
  config.rpc.host,
  config.rpc.port
).run();
