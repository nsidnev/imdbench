defmodule Bench.Runner.Reporter do
  use GenServer

  defmodule State do
    defstruct [
      :nsamples,
      :concurrency,
      reported: 0,
      queries: 0,
      min_latency: :infinity,
      max_latency: 0.0,
      latency_stats: nil,
      samples: []
    ]
  end

  def start_link(nsamples, concurrency) do
    GenServer.start_link(__MODULE__, {nsamples, concurrency}, name: __MODULE__)
  end

  def report(run_queries, run_latency_stats, run_min_latency, run_max_latency, run_start) do
    report =
      GenServer.call(
        __MODULE__,
        {:report, run_queries, run_latency_stats, run_min_latency, run_max_latency, run_start},
        :infinity
      )

    if not is_nil(report) do
      report
      |> Jason.encode!()
      |> IO.puts()
    end
  end

  def update_samples(data) do
    GenServer.cast(__MODULE__, {:update_samples, data})
  end

  def init({nsamples, concurrency}) do
    {:ok, %State{nsamples: nsamples, concurrency: concurrency}}
  end

  def handle_call(
        {:report, run_queries, run_latency_stats, run_min_latency, run_max_latency, run_start},
        _from,
        state
      ) do
    {report, state} =
      create_report(
        state,
        run_queries,
        run_latency_stats,
        run_min_latency,
        run_max_latency,
        run_start
      )

    {:reply, report, state}
  end

  def handle_cast({:update_samples, data}, state) do
    state =
      if length(state.samples) < state.nsamples do
        %State{state | samples: [data | state.samples]}
      else
        state
      end

    {:noreply, state}
  end

  defp create_report(
         %State{} = result,
         run_queries,
         run_latency_stats,
         run_min_latency,
         run_max_latency,
         run_start
       ) do
    result = %State{
      result
      | queries: result.queries + run_queries,
        max_latency: max(run_max_latency, result.max_latency),
        min_latency: min(run_min_latency, result.min_latency),
        reported: result.reported + 1,
        latency_stats:
          if is_nil(result.latency_stats) do
            run_latency_stats
          else
            result.latency_stats
            |> Enum.zip(run_latency_stats)
            |> Enum.map(fn {result_stat, run_stat} ->
              result_stat + run_stat
            end)
          end
    }

    if result.reported == result.concurrency do
      run_end = now()

      report = %{
        nqueries: result.queries,
        duration: System.convert_time_unit(run_end - run_start, :microsecond, :second),
        min_latency: result.min_latency,
        max_latency: result.max_latency,
        latency_stats: result.latency_stats,
        samples:
          result.samples
          |> Enum.reverse()
          |> Enum.slice(0, result.nsamples)
      }

      {report, result}
    else
      {nil, result}
    end
  end

  defp now do
    System.monotonic_time(:microsecond)
  end
end
