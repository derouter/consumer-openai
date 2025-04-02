import { sql } from "drizzle-orm";
import { type Transaction } from "../../drizzle.js";

export default async function up(tx: Transaction) {
  await tx.run(sql`
    CREATE TABLE jobs (
      id INTEGER PRIMARY KEY,
      offer_snapshot_id INTEGER NOT NULL --
      REFERENCES offer_snapshots (id) --
      ON DELETE RESTRICT,
      currency INTEGER NOT NULL,
      balance_delta TEXT,
      public_payload TEXT,
      private_payload TEXT,
      reason TEXT,
      reason_class INTEGER,
      created_at_local INTEGER NOT NULL,
      created_at_sync INTEGER,
      completed_at_local INTEGER,
      completed_at_sync INTEGER,
      signature_confirmed_at_local INTEGER,
      confirmation_error TEXT
    );
  `);

  await tx.run(sql`
    CREATE INDEX idx_jobs_offer_snapshot_id --
    ON jobs (offer_snapshot_id)
  `);
}
