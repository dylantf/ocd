module OCD

open Npgsql
open System
open System.IO
open FSharp.Data
open System.Data
open Dapper.FSharp.PostgreSQL

OptionTypes.register ()

[<CLIMutable>]
type AuditLog =
    { id: int
      entity: string
      action: string
      user_id: int64
      outcrop_id: int64 option
      study_id: int64 option
      inserted_at: DateTime }

let auditLogTable = table'<AuditLog> "audit_logs"

let getConnection () =
    new NpgsqlConnection("Host=localhost; Port=5432; Database=safari_api; Username=postgres; Password=postgres")
    :> IDbConnection

let filepath = Path.Combine(__SOURCE_DIRECTORY__, "dates.csv")

let csvContents () =
    CsvFile.Load(filepath, hasHeaders = true, separators = ";").Rows
    |> Seq.map (fun row -> (int64 row.["ID"], DateTime.Parse(row.["Inserted At"])))
    |> Seq.toList

let currentAuditLogs (outcropIds: int64 option list) =
    let conn = getConnection ()

    select {
        for al in auditLogTable do
            where (al.action = "created")
            andWhere (isNotNullValue al.outcrop_id)
            andWhere (isIn al.outcrop_id outcropIds)
    }
    |> conn.SelectAsync<AuditLog>

let main () =
    printfn "CSV Contents:"

    let c = csvContents ()
    c |> Seq.iter (fun row -> printfn $"{row}")

    let csvOutcropIds = List.map (fst >> Some) c

    printfn "Current audit logs:"

    currentAuditLogs (csvOutcropIds)
    |> Async.AwaitTask
    |> Async.RunSynchronously
    |> Seq.iter (fun row -> printfn $"{row}")

main ()
