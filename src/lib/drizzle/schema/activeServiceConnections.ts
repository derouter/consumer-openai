import { relations } from "drizzle-orm";
import { index, int, sqliteTable } from "drizzle-orm/sqlite-core";
import { sortByKey } from "../../util.js";
import { offerSnapshots } from "./offersSnapshots.js";

export const activeServiceConnections = sqliteTable(
  "active_service_connections",
  sortByKey({
    id: int("id").primaryKey(),

    offerSnapshotId: int("offer_snapshot_id")
      .notNull()
      .references(() => offerSnapshots.id, {
        onDelete: "restrict",
      }),

    createdAt: int("created_at", { mode: "timestamp" })
      .notNull()
      .$defaultFn(() => new Date()),
  }),
  (t) => [
    index("idx_active_service_connections_offer_snapshot_id").on(
      t.offerSnapshotId,
    ),
  ],
);

export const activeServiceConnectionRelations = relations(
  activeServiceConnections,
  ({ one, many }) => ({
    offerSnapshot: one(offerSnapshots, {
      fields: [activeServiceConnections.offerSnapshotId],
      references: [offerSnapshots.id],
    }),
  }),
);
