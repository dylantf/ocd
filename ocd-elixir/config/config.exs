import Config

config :ocd, Ocd.Repo,
  database: "safari_api",
  username: "postgres",
  password: "postgres",
  hostname: "localhost"

config :ocd, ecto_repos: [Ocd.Repo]
