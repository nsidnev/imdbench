defmodule Mix.Tasks.EdgedbEx do
  use Mix.Task

  @impl Mix.Task
  def run(args) do
    Application.ensure_all_started(:edgedb)
    Logger.configure(level: :error)

    args = Bench.parse_args(args)

    impl = Bench.EdgeDB.new(Keyword.merge(args, format: :repack))

    Bench.Runner.bench_implementation(impl, args)

    :ok
  end
end
