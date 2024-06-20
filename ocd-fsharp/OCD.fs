module OCD

open Npgsql
open System
open System.IO
open FSharp.Data
open Npgsql.FSharp

type AuditLogRead = { OutcropId: int64 }

type AuditLogInsert =
    { Entity: string
      Action: string
      UserId: int64
      OutcropId: int64
      InsertedAt: DateTime }

let connStr =
    "Host=localhost; Port=5432; Database=safari_api; Username=postgres; Password=postgres"

let filepath = Path.Combine(__SOURCE_DIRECTORY__, "dates.csv")

let csvContents () =
    CsvFile.Load(filepath, hasHeaders = true, separators = ";").Rows
    |> Seq.map (fun row -> (int64 row.["ID"], DateTime.Parse(row.["Inserted At"])))
    |> Seq.toList

let outcropIdsWithLogs searchIds =
    let query =
        @"
        select * from audit_logs
        where action = 'created'
        and outcrop_id = any(@outcropIds)"

    Sql.connect connStr
    |> Sql.query query
    |> Sql.parameters [ "outcropIds", Sql.int64Array <| List.toArray searchIds ]
    |> Sql.execute (fun read -> { OutcropId = read.int64 "outcrop_id" })
    |> Seq.map (fun log -> log.OutcropId)

let initLogsToInsert existingIds csvRecords =
    csvRecords
    |> List.filter (fun (ocId, _) -> not <| Seq.contains ocId existingIds)
    |> List.map (fun (ocId, insertedAt) ->
        { Entity = "outcrop"
          Action = "created"
          UserId = 11 // Nicole
          OutcropId = ocId
          InsertedAt = insertedAt })

let insertQuery =
    @"
    insert into audit_logs
        (entity, action, user_id, outcrop_id, inserted_at)
    values
        (@entity, @action, @userId, @outcropId, @insertedAt)
    returning id"

let rec insertNewLogs logs =
    let conn = new NpgsqlConnection(connStr)
    conn.Open()

    let rec doInsert logs success failed =
        match logs with
        | [] -> (success, failed)
        | log :: rest ->
            Sql.existingConnection conn
            |> Sql.query insertQuery
            |> Sql.parameters
                [ "entity", Sql.string log.Entity
                  "action", Sql.string log.Action
                  "userId", Sql.int64 log.UserId
                  "outcropId", Sql.int64 log.OutcropId
                  "insertedAt", Sql.date log.InsertedAt ]
            |> Sql.executeNonQuery
            |> function
                | 0 -> doInsert rest success (failed + 1)
                | n -> doInsert rest (success + n) failed

    conn.Close()
    doInsert logs 0 0

let main () =
    printfn "Loading CSV Contents..."
    let csvData = csvContents ()
    printfn "Loaded %d records from CSV" <| Seq.length csvData

    printfn "Finding outcrops that already have 'created' audit logs:"
    let curIds = csvData |> List.map fst |> outcropIdsWithLogs

    printfn "%d outcrops already have 'created' logs." <| Seq.length curIds
    printfn "Choosing outcrops that don't already have logs."
    let logsToInsert = initLogsToInsert curIds csvData

    printfn "Inserting %d new audit logs..." <| Seq.length logsToInsert

    let (success, failed) = insertNewLogs logsToInsert
    printfn $"Finished with {success} inserts and {failed} failures"

main ()
