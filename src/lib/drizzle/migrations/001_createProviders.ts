import { sql } from "drizzle-orm";
import { Transaction } from "../../drizzle.js";

export default async function up(tx: Transaction) {
  await tx.run(sql`
    CREATE TABLE providers ( --
      peer_id TEXT PRIMARY KEY,
      latest_heartbeat_at INTEGER NOT NULL
    );
  `);

  await tx.run(sql`
    CREATE INDEX idx_providers_latest_heartbeat_at --
    ON providers (latest_heartbeat_at);
  `);
}
