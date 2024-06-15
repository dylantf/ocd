let parse_csv_file =
  List.map (fun row ->
      match row with
      | [ a; b ] -> (Int64.of_string a, b)
      | _ -> failwith "CSV row formatted incorrectly")

let () =
  let filename = "file.csv" in
  let csv_data =
    Csv.load ~separator:';' filename |> List.tl |> parse_csv_file
  in
  List.iter
    (fun (oc_id, inserted_at) -> Printf.printf "%Ld %s\n" oc_id inserted_at)
    csv_data
