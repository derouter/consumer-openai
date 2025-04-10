import json5 from "json5";
import * as fs from "node:fs";
import { parseArgs } from "node:util";
import * as v from "valibot";
import { ConfigSchema } from "./config.js";
import { OpenAiConsumer } from "./consumer.js";
import { dbMigrated } from "./lib/drizzle.js";

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

dbMigrated.promise.then(() => {
  new OpenAiConsumer(config);
});
