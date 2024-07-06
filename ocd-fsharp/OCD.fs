module OCD

open Npgsql
open System
open System.IO
open FSharp.Data
open Dapper
open Dapper.FSharp.PostgreSQL

OptionTypes.register ()

let connStr =
    "Host=localhost; Port=5432; Database=safari_api; Username=postgres; Password=postgres"

let conn = new NpgsqlConnection(connStr)
conn.Open()

[<CLIMutable>]
type AuditLog =
    { id: int
      entity: string
      action: string
      user_id: int64
      outcrop_id: int64 option
      study_id: int64 option
      inserted_at: DateTime }

let auditLogsTable = table'<AuditLog> ("audit_logs")

let filepath = Path.Combine(__SOURCE_DIRECTORY__, "dates.csv")

let csvContents () =
    CsvFile.Load(filepath, hasHeaders = true, separators = ";").Rows
    |> Seq.map (fun row -> (int64 row.["ID"], DateTime.Parse(row.["Inserted At"])))
    |> Seq.toList

let outcropIdsWithLogs (searchIds: int64 list) =

    let sql =
        @"
        select * from audit_logs
        where entity = 'outcrop'
        and action = 'created'
        and outcrop_id = any(@outcropIds)"

    let parameters = new DynamicParameters()
    parameters.Add("@outcropIds", searchIds |> List.toArray)

    conn.QueryAsync<AuditLog>(sql, parameters)
    |> Async.AwaitTask
    |> Async.RunSynchronously
    |> Seq.toList
    |> List.map (fun al -> al.outcrop_id)
    |> List.choose id

let initLogsToInsert existingIds csvRecords =
    csvRecords
    |> List.filter (fun (ocId, _) -> not <| Seq.contains ocId existingIds)
    |> List.map (fun (ocId, insertedAt) ->
        { id = -1
          entity = "outcrop"
          action = "created"
          user_id = 11 // Nicole
          outcrop_id = Some ocId
          study_id = None
          inserted_at = insertedAt })

let insertNewLogs logs =
    match logs with
    | [] -> 0
    | logs ->
        insert {
            for al in auditLogsTable do
                values logs
                excludeColumn al.id
        }
        |> conn.InsertAsync
        |> Async.AwaitTask
        |> Async.RunSynchronously


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

    let success = insertNewLogs logsToInsert
    let failed = logsToInsert.Length - success
    printfn $"Finished with {success} inserts and {failed} failures"

main ()
