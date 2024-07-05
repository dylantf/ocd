open CalendarLib

let parse_csv_file filename =
  let parse_date = Printer.Calendar.from_fstring "%Y-%m-%d %H:%M:%S" in

  Csv.load filename ~separator:';'
  |> List.tl
  |> List.map (fun row ->
         match row with
         | [ outcrop_id; inserted_at ] ->
             (Int64.of_string outcrop_id, parse_date inserted_at)
         | _ -> failwith "CSV row formatted incorrectly")

let () =
  let csv_data = parse_csv_file "dates.csv" in
  List.iter
    (fun (oc_id, inserted_at) ->
      Printf.printf "%Ld %s\n" oc_id (Printer.Calendar.to_string inserted_at))
    csv_data
