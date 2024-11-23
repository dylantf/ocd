import gleam/dynamic
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/string
import pog
import simplifile
import tempo.{type DateTime}
import tempo/date
import tempo/datetime
import tempo/month
import tempo/time

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
  let sql =
    "select outcrop_id from audit_logs
     where entity = 'outcrop'
     and action = 'created'
     and outcrop_id = any($1)"

  let assert Ok(response) =
    pog.query(sql)
    |> pog.parameter(list.map(outcrop_ids, pog.int) |> pog.array)
    |> pog.returning(dynamic.element(0, dynamic.int))
    |> pog.execute(db)

  response.rows
}

fn tempo_to_pog(dt: tempo.DateTime) -> pog.Timestamp {
  let d = datetime.get_date(dt)
  let t = datetime.get_time(dt)

  let pog_date =
    pog.Date(
      date.get_year(d),
      date.get_month(d) |> month.to_int,
      date.get_day(d),
    )
  let pog_time =
    pog.Time(time.get_hour(t), time.get_minute(t), time.get_second(t), 0)

  pog.Timestamp(pog_date, pog_time)
}

fn insert_new_records(db: pog.Connection, records: List(CsvRow)) -> Int {
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
        pog.text("outcrop"),
        pog.text("created"),
        pog.int(r.outcrop_id),
        pog.int(11),
        pog.timestamp(r.inserted_at |> tempo_to_pog),
      ]
    })
    |> list.flatten

  let assert Ok(response) =
    pog.query(sql)
    |> list.fold(variables, _, pog.parameter)
    |> pog.returning(dynamic.dynamic)
    |> pog.execute(db)

  response.count
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
