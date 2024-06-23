import { and, eq, relations } from "drizzle-orm";
import {
  bigint,
  bigserial,
  pgTable,
  serial,
  text,
  timestamp,
} from "drizzle-orm/pg-core";
import { drizzle } from "drizzle-orm/postgres-js";
import Papa from "papaparse";
import postgres from "postgres";
import { z } from "zod";

//
// CSV parsey things
//

const csvContentsSchema = z
  .array(
    z.object({
      ID: z.coerce.number().int(),
      "Inserted At": z.coerce.date(),
    })
  )
  .transform(result =>
    result.map(row => ({
      outcropId: row.ID,
      insertedAt: row["Inserted At"],
    }))
  );

async function parseCsvFile(filePath: string) {
  const file = Bun.file(filePath);
  const fileContents = await file.text();

  const { data } = Papa.parse(fileContents.trim(), {
    header: true,
    delimiter: ";",
  });

  const validated = csvContentsSchema.safeParse(data);
  if (!validated.success) {
    console.log(validated.error);
    console.error("CSV malformed: expected rows 'ID','Inserted At'");
    process.exit(1);
  }

  return validated.data;
}

//
// Databasey things
//

const outcrops = pgTable("outcrop", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
});
const outcropRelations = relations(outcrops, ({ many }) => ({
  auditLogs: many(auditLogs),
}));

const auditLogs = pgTable("audit_logs", {
  id: serial("id").primaryKey(),
  entity: text("entity").notNull(),
  action: text("action").notNull(),
  userId: bigint("user_id", { mode: "number" }).notNull(),
  outcropId: bigint("outcrop_id", { mode: "number" }).references(
    () => outcrops.id
  ),
  studyId: bigint("study_id", { mode: "number" }),
  insertedAt: timestamp("inserted_at", { withTimezone: false }),
});
const auditLogRelations = relations(auditLogs, ({ one }) => ({
  outcrop: one(outcrops, {
    fields: [auditLogs.outcropId],
    references: [outcrops.id],
  }),
}));

const conn = postgres("postgres://postgres:postgres@localhost:5432/safari_api");
const db = drizzle(conn, {
  schema: {
    outcrops,
    outcropRelations,
    auditLogs,
    auditLogRelations,
  },
});

/** Check the DB for outcrops with existing 'created' logs */
async function findExistingOutcrops() {
  const cur = await db
    .select({
      id: outcrops.id,
      auditLog: auditLogs,
    })
    .from(outcrops)
    .leftJoin(
      auditLogs,
      and(
        eq(outcrops.id, auditLogs.outcropId),
        eq(auditLogs.entity, "outcrop"),
        eq(auditLogs.action, "created")
      )
    )
    .execute();

  return cur;
}

async function createMissing(records: z.infer<typeof csvContentsSchema>) {
  return db
    .insert(auditLogs)
    .values(
      records.map(r => ({
        outcropId: r.outcropId,
        insertedAt: r.insertedAt,
        entity: "outcrop",
        action: "created",
        userId: 11,
      }))
    )
    .returning({ id: auditLogs.id })
    .onConflictDoNothing();
}

async function main() {
  console.log("Parsing CSV file for available data to insert.");
  const csvContents = await parseCsvFile("dates.csv");
  console.log(`Parsed to ${csvContents.length} insertion dates.`);

  console.log("Loading existing audit logs to see which already exist.");
  const outcrops = await findExistingOutcrops();
  console.log(`Found ${outcrops.length} total outcrops.`);
  const withoutLog = outcrops.filter(oc => !oc.auditLog).map(res => res.id);
  console.log(`${withoutLog.length} outcrops are missing a 'created' log.`);

  const todos = csvContents.filter(row => withoutLog.includes(row.outcropId));

  if (todos.length > 0) {
    console.log(`Inserting ${todos.length} new audit logs.`);
    const insertRes = await createMissing(todos);
    console.log(`Inserted ${insertRes.length} records.`);
  } else {
    console.log("Nothing to do!");
  }
}

await main();
conn.end();
