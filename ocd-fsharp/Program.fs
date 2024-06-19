module OCD

open Npgsql
open System
open System.IO
open FSharp.Data
open Npgsql.FSharp

type AuditLog =
    { Id: int
      Entity: string
      Action: string
      UserId: int64
      OutcropId: int64 option
      StudyId: int64 option
      InsertedAt: DateTime }

let getConnection () =
    "Host=localhost; Port=5432; Database=safari_api; Username=postgres; Password=postgres"
    |> Sql.connect

let filepath = Path.Combine(__SOURCE_DIRECTORY__, "dates.csv")

let csvContents () =
    CsvFile.Load(filepath, hasHeaders = true, separators = ";").Rows
    |> Seq.map (fun row -> (int64 row.["ID"], DateTime.Parse(row.["Inserted At"])))
    |> Seq.toArray

let currentAuditLogs (outcropIds: int64 array) : AuditLog list =
    let q = @"
        select * from audit_logs
        where action = 'created'
        and al.outcrop_id in @outcropIds"

    getConnection()
    |> Sql.query q
    |> Sql.parameters ["outcropIds", Sql.int64Array outcropIds]
    |> Sql.executeAsync (fun read ->
        { 
            Id = read.int "id"
            Entity = read.string "entity"
            Action = read.string "action"
            UserId = read.int64 "user_id"
            OutcropId = read.int64OrNone "outcrop_id"
            StudyId = read.int64OrNone "study_id"
            InsertedAt = read.dateTime "inserted_at"
        })
    |> Async.AwaitTask
    |> Async.RunSynchronously

let main () =
    printfn "CSV Contents:"

    let c = csvContents ()
    c |> Seq.iter (fun row -> printfn $"{row}")

    let csvOutcropIds = Array.map fst c

    printfn "Current audit logs:"

    currentAuditLogs (csvOutcropIds)
    |> Seq.iter (fun row -> printfn $"{row}")

main ()
