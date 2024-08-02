import gleam/dynamic
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/pgo
import gleam/string
import simplifile

type Timestamp =
  #(#(Int, Int, Int), #(Int, Int, Int))

pub type CsvRow {
  CsvRow(outcrop_id: Int, inserted_at: Timestamp)
}

fn parse_int_exn(n: String) -> Int {
  case int.parse(n) {
    Ok(parsed) -> parsed
    _ -> panic
  }
}

fn decode_timestamp(timestamp: String) -> Timestamp {
  let assert [date, time] = string.split(timestamp, " ")

  let assert [year, month, day] =
    string.split(date, "-") |> list.map(parse_int_exn)
  let assert [hour, minute, second] =
    string.split(time, ":") |> list.map(parse_int_exn)

  #(#(year, month, day), #(hour, minute, second))
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
    let inserted_at = decode_timestamp(inserted_at)
    CsvRow(outcrop_id, inserted_at)
  })
}

fn db_config() -> pgo.Config {
  pgo.Config(
    ..pgo.default_config(),
    host: "localhost",
    port: 5432,
    database: "safari_api",
    user: "postgres",
    password: Some("postgres"),
  )
}

fn find_existing_records(
  connection: pgo.Connection,
  outcrop_ids: List(Int),
) -> List(Int) {
  let sql =
    "select outcrop_id from audit_logs
     where entity = 'outcrop'
     and action = 'created'
     and outcrop_id = any($1)"

  let query_vars = [list.map(outcrop_ids, pgo.int) |> pgo.array]

  let assert Ok(response) =
    pgo.execute(sql, connection, query_vars, dynamic.element(0, dynamic.int))

  response.rows
}

fn insert_new_records(connection: pgo.Connection, records: List(CsvRow)) -> Int {
  let placeholders =
    list.index_map(records, fn(_, i) {
      "("
      <> list.range(i * 5 + 1, i * 5 + 5)
      |> list.map(fn(num) { "$" <> int.to_string(num) })
      |> string.join(", ")
      <> ")"
    })
    |> string.join(", ")

  let sql = "insert into audit_logs
    (entity, action, outcrop_id, user_id, inserted_at)
    values " <> placeholders

  let variables =
    list.map(records, fn(r) {
      [
        pgo.text("outcrop"),
        pgo.text("created"),
        pgo.int(r.outcrop_id),
        pgo.int(11),
        pgo.timestamp(r.inserted_at),
      ]
    })
    |> list.flatten

  let assert Ok(response) =
    pgo.execute(sql, connection, variables, dynamic.dynamic)

  response.count
}

pub fn main() {
  let filename = "dates.csv"
  let assert Ok(contents) = simplifile.read(filename)
  let rows = parse_csv(contents)
  let num_rows = rows |> list.length |> int.to_string
  io.debug("Read " <> num_rows <> " records from CSV file.")

  io.println("Connecting to database...")
  let db = pgo.connect(db_config())

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
