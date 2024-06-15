defmodule Mix.Tasks.Ocd do
  use Mix.Task

  @impl Mix.Task
  def run(_) do
    Ocd.main()
  end
end
