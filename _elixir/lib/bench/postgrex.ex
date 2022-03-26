defmodule Bench.Postgrex do
  defstruct [
    :conn,
    :concurrency
  ]

  def new(opts) do
    concurrency = opts[:concurrency] || 1
    conn = create_connection(concurrency)

    %__MODULE__{
      conn: conn,
      concurrency: concurrency
    }
  end

  defp create_connection(concurrency) do
    {:ok, conn} =
      :postgrex
      |> Application.get_all_env()
      |> Keyword.merge(pool_size: concurrency)
      |> Postgrex.start_link()

    conn
  end
end

defimpl Bench.Executor, for: Bench.Postgrex do
  alias Bench.Postgrex.Queries

  def get_user(%{conn: conn}, id) do
    %Postgrex.Result{rows: [[id, name, image | _rest_row] | _rest] = rows} =
      Postgrex.query!(conn, Queries.get_user(), [id])

    Jason.encode!(%{
      id: id,
      name: name,
      image: image,
      latest_reviews:
        Enum.map(
          rows,
          fn [
               _user_id,
               _user_name,
               _user_image,
               review_id,
               review_body,
               review_rating,
               movie_id,
               movie_image,
               movie_title,
               movie_avg_rating
             ] ->
            %{
              id: review_id,
              body: review_body,
              rating: review_rating,
              movie: %{
                id: movie_id,
                image: movie_image,
                title: movie_title,
                avg_rating: Decimal.to_float(movie_avg_rating)
              }
            }
          end
        )
    })
  end

  def get_movie(%{conn: conn}, id) do
    %Postgrex.Result{
      rows: [
        [
          movie_id,
          movie_image,
          movie_title,
          movie_year,
          movie_description,
          movie_avg_rating,
          directors,
          actors,
          reviews
        ]
      ]
    } = Postgrex.query!(conn, Queries.get_movie(), [id])

    Jason.encode!(%{
      id: movie_id,
      image: movie_image,
      title: movie_title,
      year: movie_year,
      description: movie_description,
      avg_rating: Decimal.to_float(movie_avg_rating),
      directors:
        Enum.map(directors, fn {person_id, person_full_name, person_image} ->
          %{
            id: person_id,
            full_name: person_full_name,
            image: person_image
          }
        end),
      cast:
        Enum.map(actors, fn {person_id, person_full_name, person_image} ->
          %{
            id: person_id,
            full_name: person_full_name,
            image: person_image
          }
        end),
      reviews:
        Enum.map(
          reviews,
          fn {
               review_id,
               review_body,
               review_rating,
               {author_id, author_name, author_image}
             } ->
            %{
              id: review_id,
              body: review_body,
              rating: review_rating,
              author: %{
                id: author_id,
                name: author_name,
                image: author_image
              }
            }
          end
        )
    })
  end

  def get_person(%{conn: conn}, id) do
    %Postgrex.Result{
      rows: [
        [
          person_id,
          person_full_name,
          person_image,
          person_bio,
          acted_in,
          directed
        ]
      ]
    } = Postgrex.query!(conn, Queries.get_person(), [id]) |> IO.inspect()

    Jason.encode!(%{
      id: person_id,
      full_name: person_full_name,
      image: person_image,
      bio: person_bio,
      acted_in:
        Enum.map(
          acted_in,
          fn [
               movie_id,
               movie_image,
               movie_title,
               movie_year,
               movie_avg_rating
             ] ->
            %{
              id: movie_id,
              image: movie_image,
              title: movie_title,
              year: movie_year,
              avg_rating: Decimal.to_float(movie_avg_rating)
            }
          end
        ),
      directed:
        Enum.map(
          directed,
          fn [
               movie_id,
               movie_image,
               movie_title,
               movie_year,
               movie_avg_rating
             ] ->
            %{
              id: movie_id,
              image: movie_image,
              title: movie_title,
              year: movie_year,
              avg_rating: Decimal.to_float(movie_avg_rating)
            }
          end
        )
    })
  end

  def update_movie(%{conn: conn}, id) do
    suffix =
      id
      |> to_string()
      |> String.slice(0..8)

    %Postgrex.Result{rows: [[id, title]]} =
      Postgrex.query!(conn, Queries.update_movie(), [id, "---#{suffix}"])

    Jason.encode!(%{
      id: id,
      title: title
    })
  end

  def insert_user(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    %Postgrex.Result{rows: [[id, name, image]]} =
      Postgrex.query!(conn, Queries.insert_user(), ["#{value}#{num}", "image_#{value}#{num}"])

    Jason.encode!(%{
      id: id,
      name: name,
      image: image
    })
  end

  def insert_movie(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    %Postgrex.Result{
      rows: [
        [
          movie_id,
          movie_image,
          movie_title,
          movie_year,
          movie_description,
          directors,
          cast
        ]
      ]
    } =
      Postgrex.query!(conn, Queries.insert_movie(), [
        "#{value.prefix}#{num}",
        "#{value.prefix}image#{num}.jpeg",
        "#{value.prefix}description#{num}",
        num | value.people
      ])

    Jason.encode!(%{
      id: movie_id,
      title: movie_title,
      image: movie_image,
      description: movie_description,
      year: movie_year,
      directors:
        Enum.map(directors, fn {person_id, person_full_name, person_image} ->
          %{
            id: person_id,
            full_name: person_full_name,
            image: person_image
          }
        end),
      cast:
        Enum.map(cast, fn {person_id, person_full_name, person_image} ->
          %{
            id: person_id,
            full_name: person_full_name,
            image: person_image
          }
        end)
    })
  end

  def insert_movie_plus(%{conn: conn}, value) do
    num = Enum.random(0..1_000_000)

    %Postgrex.Result{
      rows: [
        [
          movie_id,
          movie_image,
          movie_title,
          movie_year,
          movie_description,
          directors,
          actors
        ]
      ]
    } =
      Postgrex.query!(conn, Queries.insert_movie_plus(), [
        "#{value.prefix}#{num}",
        "#{value.prefix}image#{num}.jpeg",
        "#{value.prefix}description#{num}",
        num,
        "#{value}Alice",
        "#{value}Director",
        "#{value}image#{num}.jpeg",
        "#{value}Billie",
        "#{value}Actor",
        "#{value}image#{num + 1}.jpeg",
        "#{value}Cameron",
        "#{value}Actor",
        "#{value}image#{num + 2}.jpeg"
      ])

    Jason.encode!(%{
      id: movie_id,
      title: movie_title,
      image: movie_image,
      description: movie_description,
      year: movie_year,
      directors:
        Enum.map(directors, fn {person_id, person_full_name, person_image} ->
          %{
            id: person_id,
            full_name: person_full_name,
            image: person_image
          }
        end),
      cast:
        Enum.map(actors, fn {person_id, person_full_name, person_image} ->
          %{
            id: person_id,
            full_name: person_full_name,
            image: person_image
          }
        end)
    })
  end
end

defimpl Bench.Implementation, for: Bench.Postgrex do
  alias Bench.Executor

  @insert_prefix "insert_test__"

  def execute(executor, "get_user", id) do
    Executor.get_user(executor, id)
  end

  def execute(executor, "get_movie", id) do
    Executor.get_movie(executor, id)
  end

  def execute(executor, "get_person", id) do
    Executor.get_person(executor, id)
  end

  def execute(executor, "update_movie", id) do
    Executor.update_movie(executor, id)
  end

  def execute(executor, "insert_user", id) do
    Executor.insert_user(executor, id)
  end

  def execute(executor, "insert_movie", id) do
    Executor.insert_movie(executor, id)
  end

  def execute(executor, "insert_movie_plus", id) do
    Executor.insert_movie_plus(executor, id)
  end

  def get_ids(%{conn: conn, concurrency: concurrency}) do
    users =
      conn
      |> Postgrex.query!("SELECT u.id FROM users u ORDER BY random();", [])
      |> Map.fetch!(:rows)
      |> Enum.reduce([], fn [id], ids ->
        [id | ids]
      end)

    people =
      conn
      |> Postgrex.query!("SELECT m.id FROM movies m ORDER BY random();", [])
      |> Map.fetch!(:rows)
      |> Enum.reduce([], fn [id], ids ->
        [id | ids]
      end)

    Postgrex.query!(conn, "SELECT p.id FROM persons p ORDER BY random();", [])

    movies =
      conn
      |> Postgrex.query!("SELECT m.id FROM movies m ORDER BY random();", [])
      |> Map.fetch!(:rows)
      |> Enum.reduce([], fn [id], ids ->
        [id | ids]
      end)

    %{
      get_user: users,
      get_person: people,
      get_movie: movies,
      update_movie: movies,
      insert_user: List.duplicate(@insert_prefix, concurrency),
      insert_movie:
        List.duplicate(
          %{
            prefix: @insert_prefix,
            people: Enum.slice(people, 1..4)
          },
          concurrency
        ),
      insert_movie_plus: List.duplicate(@insert_prefix, concurrency)
    }
  end

  def setup(%{conn: conn}, "update_movie") do
    Postgrex.query!(
      conn,
      """
      UPDATE
          movies
      SET
          title = split_part(movies.title, '---', 1)
      WHERE
          movies.title LIKE '%---%';
      """,
      []
    )
  end

  def setup(%{conn: conn}, "insert_user") do
    Postgrex.query!(
      conn,
      """
      DELETE FROM
          users
      WHERE
          users.name LIKE $1;
      """,
      ["#{@insert_prefix}%"]
    )
  end

  def setup(%{conn: conn}, query_name)
      when query_name in ~w(insert_movie insert_movie_plus) do
    Postgrex.query!(
      conn,
      """
      DELETE FROM
          "directors" as D
      USING
          "movies" as M
      WHERE
          D.movie_id = M.id AND M.image LIKE $1;
      """,
      ["#{@insert_prefix}%"]
    )

    Postgrex.query!(
      conn,
      """
      DELETE FROM
          "actors" as A
      USING
          "movies" as M
      WHERE
          A.movie_id = M.id AND M.image LIKE $1;
      """,
      ["#{@insert_prefix}%"]
    )

    Postgrex.query!(
      conn,
      """
      DELETE FROM
          "movies" as M
      WHERE
          M.image LIKE $1;
      """,
      ["#{@insert_prefix}%"]
    )

    Postgrex.query!(
      conn,
      """
      DELETE FROM
          "persons" as P
      WHERE
          P.image LIKE $1;
      """,
      ["#{@insert_prefix}%"]
    )
  end

  def setup(_executor, _query_name) do
    :ok
  end

  def cleanup(executor, query_name) do
    setup(executor, query_name)
  end
end
