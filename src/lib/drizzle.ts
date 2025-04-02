import { Deferred } from "@derouter/rpc/util";
import { drizzle } from "drizzle-orm/sqlite-proxy";
import { DatabaseSync } from "node:sqlite";
import * as schema from "./drizzle/schema.js";
import { pick } from "./util.js";

export type Transaction = Parameters<Parameters<typeof d.db.transaction>[0]>[0];
const sqlite = new DatabaseSync(":memory:");

/**
 * ADHOC: A `node:sqlite` Drizzle wrapper until https://github.com/drizzle-team/drizzle-orm/pull/4346 is merged.
 * @see https://github.com/drizzle-team/drizzle-orm/pull/4346#issuecomment-2766792806
 */
const db = drizzle<typeof schema>(
  async (sql, params, method) => {
    // console.debug({ sql, params, method });
    let stmt = sqlite.prepare(sql);

    switch (method) {
      case "all": {
        const rows = stmt.all(...params);
        // console.debug({ rows });
        return {
          rows: rows.map((row) => Object.values(row as any)),
        };
      }

      case "get": {
        const row = stmt.get(...params);
        // console.debug({ row });
        return { rows: [Object.values(row as any)] };
      }

      case "run":
      case "values":
        stmt.run(...params);
        return { rows: [] };
    }
  },

  // Pass the schema to the drizzle instance
  { schema },
);

export const d = {
  db,
  ...pick(schema, [
    "activeServiceConnections",
    "jobs",
    "offerSnapshots",
    "providers",
  ]),
};

import createProviders from "./drizzle/migrations/001_createProviders.js";
import createOfferSnapshots from "./drizzle/migrations/002_createOfferSnapshots.js";
import createActiveServiceConnections from "./drizzle/migrations/003_createActiveServiceConnections.js";
import createJobs from "./drizzle/migrations/004_createJobs.js";

export const dbMigrated = new Deferred<true>();

d.db
  .transaction(async (tx) => {
    await createProviders(tx);
    await createOfferSnapshots(tx);
    await createActiveServiceConnections(tx);
    await createJobs(tx);
  })
  .then(() => {
    console.debug("Migrated DB");
    dbMigrated.resolve(true);
  });
