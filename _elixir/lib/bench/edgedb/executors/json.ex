defmodule Bench.EdgeDB.Executors.JSON do
  defstruct [:conn]
end

defimpl Bench.Executor, for: Bench.EdgeDB.Executors.JSON do
  alias Bench.EdgeDB.Queries

  def get_user(%{conn: conn}, id) do
    EdgeDB.query_single_json!(conn, Queries.get_user(), id: id)
  end

  def get_movie(%{conn: conn}, id) do
    EdgeDB.query_single_json!(conn, Queries.get_movie(), id: id)
  end

  def get_person(%{conn: conn}, id) do
    EdgeDB.query_single_json!(conn, Queries.get_person(), id: id)
  end

  def update_movie(%{conn: conn}, id) do
    suffix =
      id
      |> to_string()
      |> String.slice(0..8)

    EdgeDB.query_single_json!(conn, Queries.update_movie(), id: id, suffix: suffix)
  end

  def insert_user(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    EdgeDB.query_single_json!(
      conn,
      Queries.insert_user(),
      name: "#{value}#{num}",
      image: "image_#{value}#{num}"
    )
  end

  def insert_movie(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    EdgeDB.query_single_json!(
      conn,
      Queries.insert_movie(),
      title: "#{value.prefix}#{num}",
      image: "#{value.prefix}image#{num}.jpeg",
      description: "#{value.prefix}description#{num}",
      year: num,
      d_id: Enum.at(value.people, 0),
      cast: Enum.slice(value.people, 1..4)
    )
  end

  def insert_movie_plus(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    EdgeDB.query_single_json!(
      conn,
      Queries.insert_movie(),
      title: "#{value}#{num}",
      image: "#{value}image#{num}.jpeg",
      description: "#{value}description#{num}",
      year: num,
      dfn: "#{value}Alice",
      dln: "#{value}Director",
      dimg: "#{value}image#{num}.jpeg",
      cfn0: "#{value}Billie",
      cln0: "#{value}Actor",
      cimg0: "#{value}image#{num + 1}.jpeg",
      cfn1: "#{value}Cameron",
      cln1: "#{value}Actor",
      cimg1: "#{value}image#{num + 2}.jpeg"
    )
  end
end
