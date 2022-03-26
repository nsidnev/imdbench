defmodule Mix.Tasks.EctoPostgrex do
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    Application.ensure_all_started(:ecto)
    Application.ensure_all_started(:postgrex)

    Logger.configure(level: :error)

    args = Bench.parse_args(args)

    impl = Bench.Ecto.new(args)

    Bench.Runner.bench_implementation(impl, args)

    :ok
  end
end
