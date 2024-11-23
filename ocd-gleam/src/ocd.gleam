import cake/adapter/postgres
import cake/insert as i
import cake/select as s
import cake/where as w
import gleam/dynamic
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/string
import pog
import simplifile
import tempo.{type DateTime}
import tempo/datetime

pub type CsvRow {
  CsvRow(outcrop_id: Int, inserted_at: DateTime)
}

fn parse_csv(contents: String) -> List(CsvRow) {
  contents
  |> string.trim
  |> string.split("\r\n")
  // File contains a header row
  |> list.drop(1)
  |> list.map(fn(row) {
    let assert [outcrop_id_s, inserted_at] = string.split(row, ";")
    let assert Ok(outcrop_id) = int.parse(outcrop_id_s)
    let assert Ok(inserted_at) =
      datetime.parse(inserted_at <> "Z", "YYYY-MM-DD HH:mm:ssZ")
    CsvRow(outcrop_id, inserted_at)
  })
}

fn db_config() -> pog.Config {
  pog.Config(
    ..pog.default_config(),
    host: "localhost",
    port: 5432,
    database: "safari_api",
    user: "postgres",
    password: Some("postgres"),
  )
}

fn find_existing_records(
  db: pog.Connection,
  outcrop_ids: List(Int),
) -> List(Int) {
  let query =
    s.new()
    |> s.select_col("outcrop_id")
    |> s.from_table("audit_logs")
    |> s.where(w.col("entity") |> w.eq(w.string("outcrop")))
    |> s.where(w.col("action") |> w.eq(w.string("created")))
    |> s.where(w.col("outcrop_id") |> w.in(outcrop_ids |> list.map(w.int)))
    |> s.to_query

  let assert Ok(result) =
    query
    |> postgres.run_read_query(dynamic.element(0, dynamic.int), db)

  result
}

fn insert_new_records(db: pog.Connection, records: List(CsvRow)) -> Int {
  let rows =
    list.map(records, fn(r) {
      [
        i.string("outcrop"),
        i.string("created"),
        i.int(r.outcrop_id),
        i.int(11),
        // Not working
      // i.string(r.inserted_at |> datetime.to_string),
      // i.string("2024-11-23T00:00:00Z"),
      ]
      |> i.row
    })

  let query =
    rows
    |> i.from_values(table_name: "audit_logs", columns: [
      "entity", "action", "outcrop_id", "user_id", "inserted_at",
    ])
    |> i.to_query

  let assert Ok(result) = postgres.run_write_query(query, dynamic.dynamic, db)

  list.length(result)
}

pub fn main() {
  let filename = "dates.csv"
  let assert Ok(contents) = simplifile.read(filename)
  let rows = parse_csv(contents)
  let num_rows = rows |> list.length |> int.to_string
  io.debug("Read " <> num_rows <> " records from CSV file.")

  io.println("Connecting to database...")
  let db = pog.connect(db_config())

  let outcrop_ids_with_existing =
    find_existing_records(db, list.map(rows, fn(row) { row.outcrop_id }))
  let existing_count = outcrop_ids_with_existing |> list.length |> int.to_string
  io.println("Found " <> existing_count <> " outcrops with existing logs.")

  let rows_to_insert =
    rows
    |> list.filter(fn(row) {
      !list.contains(outcrop_ids_with_existing, row.outcrop_id)
    })
  let num_to_insert = rows_to_insert |> list.length |> int.to_string
  io.println("Inserting " <> num_to_insert <> " new records.")

  let inserted = case list.length(rows_to_insert) {
    0 -> 0
    _ -> insert_new_records(db, rows_to_insert)
  }
  io.println("Inserted " <> int.to_string(inserted) <> " new audit logs.")
}
