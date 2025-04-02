import * as openaiProtocol from "@derouter/protocol-openai";
import { relations } from "drizzle-orm";
import {
  index,
  int,
  real,
  sqliteTable,
  text,
  unique,
} from "drizzle-orm/sqlite-core";
import { sortByKey } from "../../util.js";
import { activeServiceConnections } from "./activeServiceConnections.js";
import { jobs } from "./jobs.js";
import { providers } from "./providers.js";

export const offerSnapshots = sqliteTable(
  "offer_snapshots",
  sortByKey({
    /**
     * The offer snapshot ID as set by the backend.
     */
    id: int("id").primaryKey(),

    /**
     * The provider's Peer ID.
     */
    providerPeerId: text("provider_peer_id")
      .notNull()
      .references(() => providers.peerId),

    /**
     * Unique per {@link providerPeerId} + {@link providerOfferId}
     * w.r.t. {@link active}.
     */
    protocolId: text("protocol_id")
      .$type<typeof openaiProtocol.ProtocolId>()
      .notNull(),

    /**
     * Unique per {@link providerPeerId} + {@link protocolId}
     * w.r.t. {@link active}.
     */
    providerOfferId: text("provider_offer_id").notNull(),

    /**
     * The actual raw protocol payload.
     */
    protocolPayload: text("protocol_payload", { mode: "json" })
      .$type<openaiProtocol.OfferPayload>()
      .notNull(),

    /**
     * A snapshot becomes inactive when superseded by another snapshot
     * with the same {@link protocolId} and {@link providerOfferId},
     * but different {@link protocolPayload}; or when absent
     * in the recent heartbeat.
     *
     * A snapshot may become active later again.
     */
    active: int("active", { mode: "boolean" }).notNull().default(true),

    /**
     * Extracted from {@link protocolPayload} for indexing.
     */
    modelId: text("model_id").notNull(),

    /**
     * Extracted from {@link protocolPayload} for indexing.
     */
    contextSize: int("context_size").notNull(),

    /**
     * Extracted from {@link protocolPayload} for indexing, in eth.
     */
    inputTokenPricePol: real("input_token_price_pol").notNull(),

    /**
     * Extracted from {@link protocolPayload} for indexing, in eth.
     */
    outputTokenPricePol: real("output_token_price_pol").notNull(),
  }),
  (t) => [
    unique("idx_offer_snapshots_unique").on(
      t.providerPeerId,
      t.protocolId,
      t.providerOfferId,
      t.protocolPayload,
    ),
    index("idx_offer_snapshots_active").on(t.active),
    index("idx_offer_snapshots_model_id").on(t.modelId),
    index("idx_offer_snapshots_context_size").on(t.contextSize),
    index("idx_offer_snapshots_input_token_price_pol").on(t.inputTokenPricePol),
    index("idx_offer_snapshots_output_token_price_pol").on(
      t.outputTokenPricePol,
    ),
  ],
);

export const offerSnapshotRelations = relations(
  offerSnapshots,
  ({ one, many }) => ({
    provider: one(providers, {
      fields: [offerSnapshots.providerPeerId],
      references: [providers.peerId],
    }),

    activeServiceConnections: many(activeServiceConnections),
    jobs: many(jobs),
  }),
);
