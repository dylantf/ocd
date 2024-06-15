module OCD

open System
open System.IO
open FSharp.Data

let filepath =
    Path.Combine(__SOURCE_DIRECTORY__, "Outcrop_creationDates_.InsertedAt.csv")

let contents =
    CsvFile.Load(filepath, hasHeaders = true, separators = ";").Rows
    |> Seq.map (fun row -> (int row.["ID"], DateTime.Parse(row.["Inserted At"])))
