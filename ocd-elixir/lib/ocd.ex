defmodule Ocd do
  import Ecto.Query
  alias Ocd.Repo
  alias Ocd.Db.AuditLog

  def main do
    records = parse_csv_file()
    outcrop_ids = Enum.map(records, fn {id, _} -> id end)
    IO.puts("Found #{length(outcrop_ids)} outcrop IDs")

    logs = existing_audit_logs(outcrop_ids)
    IO.puts("Loaded #{length(logs)} existing audit logs!")
  end

  defp parse_csv_file do
    File.stream!("dates.csv")
    |> CSV.decode(separator: ?;, headers: true)
    |> Stream.map(&parse_csv_row/1)
    |> Enum.to_list()
  end

  defp parse_csv_row({:ok, %{"ID" => id, "Inserted At" => inserted_at}}) do
    {id, _} = Integer.parse(id)
    inserted_at = Timex.parse!(inserted_at, "%Y-%m-%d %H:%M:%S", :strftime)
    {id, inserted_at}
  end

  defp parse_csv_row(_), do: raise("Error parsing CSV row")

  defp existing_audit_logs(ids) do
    from(al in AuditLog,
      where: al.outcrop_id in ^ids and al.action == "created" and al.entity == "outcrop"
    )
    |> Repo.all()
  end
end
