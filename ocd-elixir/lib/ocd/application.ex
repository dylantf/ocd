defmodule Ocd.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [Ocd.Repo]

    opts = [strategy: :one_for_one, name: Ocd.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
