import { sql } from "drizzle-orm";
import { Transaction } from "../../drizzle.js";

export default function up(tx: Transaction) {
  tx.run(sql`
    CREATE TABLE offer_snapshots (
      id INTEGER PRIMARY KEY,
      provider_peer_id TEXT NOT NULL REFERENCES providers (peer_id),
      protocol_id TEXT NOT NULL,
      provider_offer_id TEXT NOT NULL,
      protocol_payload TEXT NOT NULL,
      active INTEGER NOT NULL,
      model_id TEXT NOT NULL,
      context_size INTEGER NOT NULL,
      input_token_price_pol BLOB NOT NULL,
      output_token_price_pol BLOB NOT NULL
    );
  `);

  tx.run(sql`
    CREATE UNIQUE INDEX idx_offer_snapshots_unique --
    ON offer_snapshots ( --
      provider_peer_id,
      protocol_id,
      provider_offer_id,
      protocol_payload
    );
  `);

  tx.run(sql`
    CREATE INDEX idx_offer_snapshots_active --
    ON offer_snapshots (active);
  `);

  tx.run(sql`
    CREATE INDEX idx_offer_snapshots_model_id --
    ON offer_snapshots (model_id);
  `);

  tx.run(sql`
    CREATE INDEX idx_offer_snapshots_context_size --
    ON offer_snapshots (context_size);
  `);

  tx.run(sql`
    CREATE INDEX idx_offer_snapshots_input_token_price_pol --
    ON offer_snapshots (input_token_price_pol);
  `);

  tx.run(sql`
    CREATE INDEX idx_offer_snapshots_output_token_price_pol --
    ON offer_snapshots (output_token_price_pol);
  `);
}
