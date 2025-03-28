import Database from "better-sqlite3";
import { drizzle } from "drizzle-orm/better-sqlite3";
import * as schema from "./drizzle/schema.js";
import { pick } from "./util.js";

export type Transaction = Parameters<Parameters<typeof d.db.transaction>[0]>[0];

export const d = {
  db: drizzle(new Database(":memory:"), { schema }),
  ...pick(schema, ["activeServiceConnections", "offerSnapshots", "providers"]),
};

import createProviders from "./drizzle/migrations/001_createProviders.js";
import createOfferSnapshots from "./drizzle/migrations/002_createOfferSnapshots.js";
import createActiveServiceConnections from "./drizzle/migrations/003_createActiveServiceConnections.js";

d.db.transaction(async (tx) => {
  createProviders(tx);
  createOfferSnapshots(tx);
  createActiveServiceConnections(tx);
});
