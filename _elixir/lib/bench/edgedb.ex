defmodule Bench.EdgeDB do
  alias Bench.EdgeDB.Executors

  defstruct [
    :executor,
    :concurrency
  ]

  def new(opts) do
    concurrency = opts[:concurrency] || 1
    conn = create_connection(concurrency)

    executor =
      case opts[:format] do
        :json ->
          %Executors.JSON{conn: conn}

        :repack ->
          %Executors.Repack{conn: conn}
      end

    %__MODULE__{
      executor: executor,
      concurrency: concurrency
    }
  end

  defp create_connection(concurrency) do
    {:ok, conn} = EdgeDB.start_link(pool_size: concurrency)

    conn
  end
end

defimpl Bench.Implementation, for: Bench.EdgeDB do
  alias Bench.Executor

  @insert_prefix "insert_test__"

  def execute(%{executor: executor}, "get_user", id) do
    Executor.get_user(executor, id)
  end

  def execute(%{executor: executor}, "get_movie", id) do
    Executor.get_movie(executor, id)
  end

  def execute(%{executor: executor}, "get_person", id) do
    Executor.get_person(executor, id)
  end

  def execute(%{executor: executor}, "update_movie", id) do
    Executor.update_movie(executor, id)
  end

  def execute(%{executor: executor}, "insert_user", id) do
    Executor.insert_user(executor, id)
  end

  def execute(%{executor: executor}, "insert_movie", id) do
    Executor.insert_movie(executor, id)
  end

  def execute(%{executor: executor}, "insert_movie_plus", id) do
    Executor.insert_movie_plus(executor, id)
  end

  def get_ids(%{executor: %{conn: conn}, concurrency: concurrency}) do
    ids =
      EdgeDB.query_single!(conn, """
      WITH
          U := User {id, r := random()},
          M := Movie {id, r := random()},
          P := Person {id, r := random()}
      SELECT (
          users := array_agg((SELECT U ORDER BY U.r).id),
          movies := array_agg((SELECT M ORDER BY M.r).id),
          people := array_agg((SELECT P ORDER BY P.r).id),
      );
      """)

    %{
      get_user: ids[:users],
      get_person: ids[:people],
      get_movie: ids[:movies],
      update_movie: ids[:movies],
      insert_user: List.duplicate(@insert_prefix, concurrency),
      insert_movie:
        List.duplicate(
          %{
            prefix: @insert_prefix,
            people: Enum.slice(ids[:people], 1..4)
          },
          concurrency
        ),
      insert_movie_plus: List.duplicate(@insert_prefix, concurrency)
    }
  end

  def setup(%{executor: %{conn: conn}}, "update_movie") do
    EdgeDB.query!(
      conn,
      """
      update Movie
      filter contains(.title, '---')
      set {
          title := str_split(.title, '---')[0]
      };
      """
    )
  end

  def setup(%{executor: %{conn: conn}}, "insert_user") do
    EdgeDB.query!(
      conn,
      """
      delete User
      filter .name LIKE <str>$prefix
      """,
      prefix: "#{@insert_prefix}%"
    )
  end

  def setup(%{executor: %{conn: conn}}, "insert_movie") do
    EdgeDB.query!(
      conn,
      """
      delete Movie
      filter .image LIKE <str>$prefix
      """,
      prefix: "#{@insert_prefix}image%"
    )
  end

  def setup(%{executor: %{conn: conn}}, "insert_movie_plus") do
    EdgeDB.query!(
      conn,
      """
      delete Movie
      filter .image LIKE <str>$prefix
      """,
      prefix: "#{@insert_prefix}image%"
    )

    EdgeDB.query!(
      conn,
      """
      delete Person
      filter .image LIKE <str>$prefix
      """,
      prefix: "#{@insert_prefix}image%"
    )
  end

  def setup(_executor, _query_name) do
    :ok
  end

  def cleanup(executor, query_name) do
    setup(executor, query_name)
  end
end
