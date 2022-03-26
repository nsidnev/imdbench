defmodule Bench.EdgeDB.Executors.Repack do
  defstruct [:conn]
end

defimpl Bench.Executor, for: Bench.EdgeDB.Executors.Repack do
  alias Bench.EdgeDB.Queries

  def get_user(%{conn: conn}, id) do
    u = EdgeDB.query_single!(conn, Queries.get_user(), id: id)

    Jason.encode!(%{
      id: u[:id],
      name: u[:name],
      image: u[:image],
      latest_reviews:
        Enum.map(u[:latest_reviews], fn r ->
          %{
            id: r[:id],
            body: r[:body],
            rating: r[:rating],
            movie: %{
              id: r[:movie][:id],
              image: r[:movie][:image],
              title: r[:movie][:title],
              avg_rating: r[:movie][:avg_rating]
            }
          }
        end)
    })
  end

  def get_movie(%{conn: conn}, id) do
    m = EdgeDB.query_single!(conn, Queries.get_movie(), id: id)

    Jason.encode!(%{
      id: m[:id],
      image: m[:image],
      title: m[:title],
      year: m[:year],
      description: m[:description],
      avg_rating: m[:avg_rating],
      directors:
        Enum.map(m[:directors], fn d ->
          %{
            id: d[:id],
            full_name: d[:full_name],
            image: d[:image]
          }
        end),
      cast:
        Enum.map(m[:cast], fn c ->
          %{
            id: c[:id],
            full_name: c[:full_name],
            image: c[:image]
          }
        end),
      reviews:
        Enum.map(m[:reviews], fn r ->
          %{
            id: r[:id],
            body: r[:body],
            rating: r[:rating],
            author: %{
              id: r[:author][:id],
              name: r[:author][:name],
              image: r[:author][:image]
            }
          }
        end)
    })
  end

  def get_person(%{conn: conn}, id) do
    p = EdgeDB.query_single!(conn, Queries.get_person(), id: id)

    Jason.encode!(%{
      id: p[:id],
      full_name: p[:full_name],
      image: p[:image],
      bio: p[:bio],
      acted_in:
        Enum.map(p[:acted_in], fn m ->
          %{
            id: m[:id],
            image: m[:image],
            title: m[:title],
            year: m[:year],
            avg_rating: m[:avg_rating]
          }
        end),
      directed:
        Enum.map(p[:directed], fn m ->
          %{
            id: m[:id],
            image: m[:image],
            title: m[:title],
            year: m[:year],
            avg_rating: m[:avg_rating]
          }
        end)
    })
  end

  def update_movie(%{conn: conn}, id) do
    suffix =
      id
      |> to_string()
      |> String.slice(0..8)

    u = EdgeDB.query_single_json!(conn, Queries.update_movie(), id: id, suffix: suffix)

    Jason.encode!(%{
      id: u[:id],
      title: u[:title]
    })
  end

  def insert_user(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    u =
      EdgeDB.query_single!(
        conn,
        Queries.insert_user(),
        name: "#{value}#{num}",
        image: "image_#{value}#{num}"
      )

    Jason.encode!(%{
      id: u[:id],
      name: u[:name],
      title: u[:title]
    })
  end

  def insert_movie(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    m =
      EdgeDB.query_single!(
        conn,
        Queries.insert_movie(),
        title: "#{value.prefix}#{num}",
        image: "#{value.prefix}image#{num}.jpeg",
        description: "#{value.prefix}description#{num}",
        year: num,
        d_id: Enum.at(value.people, 0),
        cast: Enum.slice(value.people, 1..4)
      )

    Jason.encode!(%{
      id: m[:id],
      image: m[:image],
      title: m[:title],
      year: m[:year],
      description: m[:description],
      directors:
        Enum.map(m[:directors], fn d ->
          %{
            id: d[:id],
            full_name: d[:full_name],
            image: d[:image]
          }
        end),
      cast:
        Enum.map(m[:cast], fn c ->
          %{
            id: c[:id],
            full_name: c[:full_name],
            image: c[:image]
          }
        end)
    })
  end

  def insert_movie_plus(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    m =
      EdgeDB.query_single!(
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

    Jason.encode!(%{
      id: m[:id],
      image: m[:image],
      title: m[:title],
      year: m[:year],
      description: m[:description],
      directors:
        Enum.map(m[:directors], fn d ->
          %{
            id: d[:id],
            full_name: d[:full_name],
            image: d[:image]
          }
        end),
      cast:
        Enum.map(m[:cast], fn c ->
          %{
            id: c[:id],
            full_name: c[:full_name],
            image: c[:image]
          }
        end)
    })
  end
end
