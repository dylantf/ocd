defmodule Ocd do
  import Ecto.Query
  alias Ocd.Repo
  alias Ocd.Db.AuditLog

  @type csv_row :: {integer(), DateTime.t()}

  def main do
    records = parse_csv_file()
    outcrop_ids = Enum.map(records, fn {id, _} -> id end)
    IO.puts("Parsed #{length(records)} records from CSV file")

    logs = existing_audit_logs(outcrop_ids)
    IO.puts("Loaded #{length(logs)} existing audit logs!")

    to_insert = logs_to_insert(records, logs)
    IO.puts("#{length(to_insert)} audit logs need to be created")

    result = create_audit_logs(to_insert)
    successful = Enum.count(result, fn {_id, status} -> status == :ok end)
    errors = Enum.count(result, fn {_id, status} -> status == :error end)
    IO.puts("Finished with #{successful} and #{errors} errors")
    nil
  end

  @spec parse_csv_file() :: [csv_row()]
  defp parse_csv_file do
    File.stream!("dates.csv")
    |> CSV.decode(separator: ?;, headers: true)
    |> Stream.map(&parse_csv_row/1)
    |> Enum.to_list()
  end

  @spec parse_csv_row({:ok, map()} | any()) :: csv_row()
  defp parse_csv_row({:ok, %{"ID" => id, "Inserted At" => inserted_at}}) do
    {id, _} = Integer.parse(id)
    inserted_at = Timex.parse!(inserted_at, "%Y-%m-%d %H:%M:%S", :strftime)
    {id, inserted_at}
  end

  defp parse_csv_row(_), do: raise("Error parsing CSV row")

  @spec existing_audit_logs([integer()]) :: [%AuditLog{}]
  defp existing_audit_logs(ids) do
    from(al in AuditLog,
      where: al.outcrop_id in ^ids and al.action == "created" and al.entity == "outcrop"
    )
    |> Repo.all()
  end

  @spec logs_to_insert([csv_row()], [%AuditLog{}]) :: [csv_row()]
  defp logs_to_insert(csv_data, existing_logs) do
    existing_ids = Enum.map(existing_logs, fn log -> log.outcrop_id end)
    Enum.filter(csv_data, fn {id, _} -> !Enum.member?(existing_ids, id) end)
  end

  @type create_result :: {integer(), atom()}

  @spec create_audit_logs([csv_row()], [create_result()]) :: [create_result()]

  defp create_audit_logs(items, acc \\ [])
  defp create_audit_logs([], acc), do: acc

  defp create_audit_logs([{outcrop_id, inserted_at} | rest], acc) do
    Repo.insert(%AuditLog{
      entity: "outcrop",
      action: "created",
      user_id: 11,
      outcrop_id: outcrop_id,
      study_id: nil,
      inserted_at: inserted_at
    })
    |> case do
      {:ok, _} ->
        IO.puts("Create log for #{outcrop_id} created successfully")
        create_audit_logs(rest, [{outcrop_id, :ok} | acc])

      err ->
        IO.inspect(err, label: "Error creating audit log - Outcrop #{outcrop_id}")
        create_audit_logs(rest, [{outcrop_id, :error} | acc])
    end
  end
end
