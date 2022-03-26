defmodule Bench.Runner.IndexAgent do
  use Agent

  def start_link(ids) do
    Agent.start_link(fn -> {0, ids} end, name: __MODULE__)
  end

  def get_and_move_next do
    Agent.get_and_update(
      __MODULE__,
      fn {idx, ids} ->
        {
          Enum.at(ids, idx),
          {rem(idx + 1, length(ids)), ids}
        }
      end,
      :infinity
    )
  end
end
