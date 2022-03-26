defmodule Bench.Runner do
  alias Bench.Implementation
  alias Bench.Runner.{Reporter, IndexAgent}

  def bench_implementation(impl, args) do
    timeout_in_ms = System.convert_time_unit(args[:timeout], :second, :microsecond)

    Reporter.start_link(args[:nsamples], args[:concurrency])

    ids = Implementation.get_ids(impl)[String.to_existing_atom(args[:query])]

    ids =
      if length(ids) > args[:number_of_ids] do
        Enum.slice(0, args[:number_of_ids])
      else
        ids
      end

    IndexAgent.start_link(ids)

    if args[:warmup_time] do
      run(
        impl,
        args[:query],
        args[:concurrency],
        args[:warmup_time],
        false,
        timeout_in_ms
      )
    end

    run(
      impl,
      args[:query],
      args[:concurrency],
      args[:duration],
      true,
      timeout_in_ms
    )
  end

  defp run(impl, query, concurrency, run_duration, report?, timeout_in_ms) do
    run_start = now()

    Implementation.setup(impl, query)

    1..concurrency
    |> Enum.map(fn _idx ->
      Task.async(fn ->
        {queries, latency_stats, min_latency, max_latency} =
          do_run(impl, query, run_duration, run_start, timeout_in_ms)

        if report? do
          Reporter.report(queries, latency_stats, min_latency, max_latency, run_start)
        end
      end)
    end)
    |> Task.await_many(:infinity)

    Implementation.cleanup(impl, query)
  end

  defp do_run(impl, query, run_duration, run_start, timeout_in_ms) do
    queries = 0
    latency_stats = List.duplicate(0, trunc(timeout_in_ms / 10))
    min_latency = :infinity
    max_latency = 0.0
    duration_in_ms = System.convert_time_unit(run_duration, :second, :microsecond)

    do_run(
      impl,
      query,
      queries,
      latency_stats,
      min_latency,
      max_latency,
      duration_in_ms,
      run_start
    )
  end

  defp do_run(
         impl,
         query,
         queries,
         latency_stats,
         min_latency,
         max_latency,
         duration_in_ms,
         run_start
       ) do
    req_start = now()
    id = IndexAgent.get_and_move_next()

    data = Implementation.execute(impl, query, id)

    Reporter.update_samples(data)

    req_time = round((now() - req_start) / 10)

    max_latency = max(req_time, max_latency)
    min_latency = min(req_time, min_latency)

    latency_stats = List.replace_at(latency_stats, req_time, Enum.at(latency_stats, req_time) + 1)
    queries = queries + 1

    if now() - run_start < duration_in_ms do
      do_run(
        impl,
        query,
        queries,
        latency_stats,
        min_latency,
        max_latency,
        duration_in_ms,
        run_start
      )
    else
      {queries, latency_stats, min_latency, max_latency}
    end
  end

  defp now do
    System.monotonic_time(:microsecond)
  end
end
