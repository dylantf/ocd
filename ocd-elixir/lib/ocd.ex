defmodule Ocd do
  def main do
    File.stream!("dates.csv")
    |> CSV.decode(separator: ?;, headers: true)
    |> Stream.map(&parse_row/1)
    |> Enum.to_list()
    |> IO.inspect()
  end

  defp parse_row({:ok, %{"ID" => id, "Inserted At" => inserted_at}}) do
    {id, _} = Integer.parse(id)
    inserted_at = Timex.parse!(inserted_at, "%Y-%m-%d %H:%M:%S", :strftime)
    {id, inserted_at}
  end

  defp parse_row(_), do: raise("Error parsing CSV row")
end
