import birl
import gleam/io
import gleam/list
import gleam/string
import simplifile

pub fn main() {
  let filename = "dates.csv"
  let assert Ok(contents) = simplifile.read(filename)

  let rows =
    contents
    |> string.trim
    |> string.split("\r\n")
    |> list.map(string.split(_, ";"))

  io.debug(rows)

  let assert Ok(result) = birl.parse("2020-05-17 00:00:00")
  io.debug(result |> birl.to_iso8601)
  // Nope, parses it wrong: 2020-05-01T00:00:00.000-01:07"
}
