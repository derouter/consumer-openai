import { sql } from "drizzle-orm";
import { type Transaction } from "../../drizzle.js";

export default async function up(tx: Transaction) {
  await tx.run(sql`
    CREATE TABLE active_service_connections (
      id INTEGER PRIMARY KEY,
      offer_snapshot_id INTEGER NOT NULL --
      REFERENCES offer_snapshots (id) --
      ON DELETE RESTRICT,
      --
      created_at INTEGER NOT NULL
    );
  `);

  await tx.run(sql`
    CREATE INDEX idx_active_service_connections_offer_snapshot_id --
    ON active_service_connections (offer_snapshot_id)
  `);
}
