defmodule Bench.MixProject do
  use Mix.Project

  def project do
    [
      app: :bench,
      version: "1.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:edgedb, git: "https://github.com/nsidnev/edgedb-elixir"},
      {:ecto, "~> 3.7"},
      {:ecto_sql, "~> 3.7"},
      {:postgrex, "~> 0.16.2"},
      {:jason, "~> 1.0"}
    ]
  end

  defp aliases do
    [
      "ecto.setup": ["ecto.migrate", "run priv/repo/seeds.exs"],
    ]
  end
end
