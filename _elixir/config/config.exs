import Config

config :bench,
  ecto_repos: [Bench.Ecto.Repo]

config :bench, Bench.Ecto.Repo,
  username: "ecto_bench",
  database: "ecto_bench",
  password: "edgedbbenchmark",
  hostname: "localhost",
  port: 15432

config :edgedb,
  retry: [
    transaction_conflict: [attempts: 10],
    network_error: [attempts: 10]
  ]

config :postgrex,
  username: "postgres_bench",
  database: "postgres_bench",
  password: "edgedbbenchmark",
  hostname: "localhost",
  port: 15432
