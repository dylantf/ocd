import gleam/dynamic
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/pgo
import gleam/string
import simplifile

pub type CsvRow {
  CsvRow(outcrop_id: Int, inserted_at: String)
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

pub fn main() {
  let filename = "dates.csv"
  let assert Ok(contents) = simplifile.read(filename)
  let rows = parse_csv(contents)

  let db = pgo.connect(db_config())
  let outcrop_ids_existing =
    find_existing_records(db, list.map(rows, fn(row) { row.outcrop_id }))

  io.print("Outcrops with existing logs:")
  io.debug(outcrop_ids_existing)
}
