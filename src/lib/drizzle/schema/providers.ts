import { relations } from "drizzle-orm";
import { index, int, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { sortByKey } from "../../util.js";
import { offerSnapshots } from "./offersSnapshots.js";

export const providers = sqliteTable(
  "providers",
  sortByKey({
    peerId: text("peer_id").primaryKey(),
    latestHeartbeatAt: int("latest_heartbeat_at", {
      mode: "timestamp",
    }).notNull(),
  }),
  (t) => [index("idx_providers_latest_heartbeat_at").on(t.latestHeartbeatAt)],
);

export const providerRelations = relations(providers, ({ many }) => ({
  offerSnapshots: many(offerSnapshots),
}));
