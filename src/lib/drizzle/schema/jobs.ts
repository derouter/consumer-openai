import { relations } from "drizzle-orm";
import { int, sqliteTable, text } from "drizzle-orm/sqlite-core";
import { sortByKey } from "../../util.js";
import { offerSnapshots } from "./offersSnapshots.js";

export const jobs = sqliteTable(
  "jobs",
  sortByKey({
    id: int("id").primaryKey(),
    offerSnapshotId: int("offer_snapshot_id")
      .references(() => offerSnapshots.id, { onDelete: "restrict" })
      .notNull(),
    currency: int("currency").notNull(),
    balanceDelta: text("balance_delta"),
    publicPayload: text("public_payload"),
    privatePayload: text("private_payload"),
    reason: text("reason"),
    reasonClass: int("reason_class"),
    createdAtLocal: int("created_at_local", { mode: "timestamp" }).notNull(),
    createdAtSync: int("created_at_sync"),
    completedAtLocal: int("completed_at_local", { mode: "timestamp" }),
    completedAtSync: int("completed_at_sync"),
    signatureConfirmedAtLocal: int("signature_confirmed_at_local", {
      mode: "timestamp",
    }),
    confirmationError: text("confirmation_error"),
  }),
);

export const jobRelations = relations(jobs, ({ one, many }) => ({
  offerSnapshot: one(offerSnapshots, {
    fields: [jobs.offerSnapshotId],
    references: [offerSnapshots.id],
  }),
}));
