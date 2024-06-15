defmodule Ocd.Repo do
  use Ecto.Repo,
    otp_app: :ocd,
    adapter: Ecto.Adapters.Postgres
end
