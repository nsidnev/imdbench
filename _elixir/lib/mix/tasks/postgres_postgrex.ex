defmodule Mix.Tasks.PostgresPostgrex do
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    Application.ensure_all_started(:postgrex)

    args = Bench.parse_args(args)

    impl = Bench.Postgrex.new(args)

    Bench.Runner.bench_implementation(impl, args)

    :ok
  end
end
