defmodule Mix.Tasks.Ocd do
  use Mix.Task

  @impl Mix.Task
  def run(_) do
    Mix.Task.run("app.start")
    Ocd.main()
  end
end
