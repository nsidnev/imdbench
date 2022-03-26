defmodule DatasetLoader do
  import Ecto.Query

  alias Bench.Ecto.Repo
  alias Bench.Ecto.Schemas.{
    Cast,
    User,
    User,
    Person,
    Director,
    Movie,
    Review
  }

  def load_data(filename) do
    drop_existing_data()

    data = parse_data(filename)

    insert_data_for_schema(User, data["user"])
    insert_data_for_schema(Person, data["person"])
    insert_data_for_schema(Movie, data["movie"])
    insert_data_for_schema(Review, data["review"])
    insert_data_for_schema(Director, data["directors"])
    insert_data_for_schema(Cast, data["cast"])

    set_sequence_for_schema(User)
    set_sequence_for_schema(Person)
    set_sequence_for_schema(Movie)
    set_sequence_for_schema(Review)
    set_sequence_for_schema(Director)
    set_sequence_for_schema(Cast)
  end

  defp set_sequence_for_schema(schema) do
    last_id = Repo.one!(from r in schema, order_by: [desc: :id], limit: 1, select: r.id)

    table = schema.__schema__(:source)
    seq_name = "#{table}_id_seq"
    Ecto.Adapters.SQL.query!(Repo, "SELECT setval('#{seq_name}', $1)", [last_id])
  end

  defp insert_data_for_schema(schema, data) do
    data
      |> Enum.map(fn record ->
        Enum.into(record, %{}, fn {key, value} ->
          {String.to_existing_atom(key), value}
        end)
      end)
      |> Enum.chunk_every(1000)
      |> Enum.map(fn chunk ->
        IO.puts("insert 1000 records into #{schema}")
        Repo.insert_all(schema, chunk)
      end)
  end

  defp drop_existing_data do
    Repo.delete_all(Cast)
    Repo.delete_all(User)
    Repo.delete_all(User)
    Repo.delete_all(Person)
    Repo.delete_all(Director)
    Repo.delete_all(Movie)
  end

  defp parse_data(filename) do
    data =
      filename
      |> File.read!()
      |> Jason.decode!()

    Enum.reduce(data, %{}, fn record, acc ->
      schema =
        record["model"]
        |> String.split(".")
        |> List.last()

      fields = record["fields"]

      fields =
        case record do
          %{"pk" => value} ->
            Map.put(fields, "id", value)

          _other ->
            fields
        end

      fields =
        if schema == "review" do
          {:ok, dt, _offset} = DateTime.from_iso8601(fields["creation_time"])
          Map.put(fields, "creation_time", dt)
        else
          fields
        end

      Map.update(acc, schema, [fields], fn records ->
        [fields | records]
      end)
    end)
  end
end

Application.ensure_all_started(:ecto)
Application.ensure_all_started(:postgrex)
{:ok, _pid} = Bench.Ecto.Repo.start_link()


with [dataset_file] <- System.argv() do
  DatasetLoader.load_data(dataset_file)
end
