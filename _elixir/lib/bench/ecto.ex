defmodule Bench.Ecto do
  defstruct [
    :concurrency
  ]

  def new(opts) do
    concurrency = opts[:concurrency] || 1
    {:ok, _pid} = Bench.Ecto.Repo.start_link(pool_size: concurrency)

    %__MODULE__{
      concurrency: concurrency
    }
  end
end

defimpl Bench.Executor, for: Bench.Ecto do
  import Ecto.Query

  alias Bench.Ecto.Repo

  alias Bench.Ecto.Schemas.{
    Cast,
    User,
    Movie,
    Director,
    Person,
    Review
  }

  @nulls_last trunc(:math.pow(2, 64))

  def get_user(_executor, id) do
    user = Repo.one!(from u in User, where: u.id == ^id)

    query =
      from r in Review,
        join: m in assoc(r, :movie),
        where: r.author_id == ^user.id,
        order_by: [desc: r.creation_time],
        limit: 10,
        preload: [movie: m]

    reviews = Repo.all(query)

    Jason.encode!(%{
      id: user.id,
      name: user.name,
      image: user.name,
      latest_reviews:
        Enum.map(reviews, fn r ->
          %{
            id: r.id,
            body: r.body,
            rating: r.rating,
            movie: %{
              id: r.movie.id,
              image: r.movie.image,
              title: r.movie.title,
              avg_rating: movie_avg_rating(r.movie)
            }
          }
        end)
    })
  end

  def get_movie(_executor, id) do
    query =
      from m in Movie,
        join: d in Director,
        on: d.movie_id == ^id,
        join: c in Cast,
        on: c.movie_id == ^id,
        join: dp in assoc(d, :person),
        join: cp in assoc(c, :person),
        join: r in assoc(m, :reviews),
        join: a in assoc(r, :author),
        where: m.id == ^id,
        preload: [
          directors: {d, person: dp},
          cast: {c, person: cp},
          reviews: {r, author: a}
        ]

    movie = Repo.one!(query)

    Jason.encode!(%{
      id: movie.id,
      image: movie.image,
      title: movie.title,
      year: movie.year,
      descrription: movie.description,
      avg_rating: movie_avg_rating(movie),
      directors:
        movie.directors
        |> Enum.sort_by(fn director ->
          if is_nil(director.list_order) do
            {@nulls_last, director.person.last_name}
          else
            {director.list_order, director.person.last_name}
          end
        end)
        |> Enum.map(fn %{person: director} ->
          %{
            id: director.id,
            full_name: person_full_name(director),
            image: director.image
          }
        end),
      cast:
        movie.cast
        |> Enum.sort_by(fn cast ->
          if is_nil(cast.list_order) do
            {@nulls_last, cast.person.last_name}
          else
            {cast.list_order, cast.person.last_name}
          end
        end)
        |> Enum.map(fn %{person: cast} ->
          %{
            id: cast.id,
            full_name: person_full_name(cast),
            image: cast.image
          }
        end),
      reviews:
        movie.reviews
        |> Enum.sort_by(& &1.creation_time, :desc)
        |> Enum.map(fn review ->
          %{
            id: review.id,
            body: review.body,
            rating: review.rating,
            author: %{
              id: review.author.id,
              name: review.author.name,
              image: review.author.image
            }
          }
        end)
    })
  end

  def get_person(_executor, id) do
    query =
      from p in Person,
        left_join: d in assoc(p, :directed),
        left_join: c in assoc(p, :acted_in),
        where: p.id == ^id,
        preload: [
          directed: d,
          acted_in: c
        ]

    person = Repo.one!(query)

    Jason.encode!(%{
      id: person.id,
      full_name: person_full_name(person),
      image: person.image,
      bio: person.bio,
      acted_in:
        person.acted_in
        |> Enum.sort_by(&{&1.year, &1.title})
        |> Enum.map(fn movie ->
          %{
            id: movie.id,
            image: movie.image,
            title: movie.title,
            year: movie.year,
            avg_rating: movie_avg_rating(movie)
          }
        end),
      directed:
        person.directed
        |> Enum.sort_by(&{&1.year, &1.title})
        |> Enum.map(fn movie ->
          %{
            id: movie.id,
            image: movie.image,
            title: movie.title,
            year: movie.year,
            avg_rating: movie_avg_rating(movie)
          }
        end)
    })
  end

  def update_movie(_executor, id) do
    suffix =
      id
      |> to_string()
      |> String.slice(0..8)

    query =
      from m in Movie,
        where: m.id == ^id,
        update: [
          set: [
            title: fragment("? || ?", m.title, ^"---#{suffix}")
          ]
        ],
        select: [:id, :title]

    {1, [movie]} = Repo.update_all(query, [])

    Jason.encode!(%{
      id: movie.id,
      title: movie.title
    })
  end

  def insert_user(_executor, value) do
    num = Enum.random(0..1_000_000)

    user = Repo.insert!(%User{name: "#{value}#{num}", image: "image_#{value}#{num}"})

    Jason.encode!(%{
      id: user.id,
      name: user.name,
      image: user.image
    })
  end

  def insert_movie(_executor, %{people: [director_id, cast_id1, cast_id2, cast_id3]} = value) do
    num = Enum.random(0..1_000_000)

    movie =
      %Movie{}
      |> Ecto.Changeset.cast(
        %{
          title: "#{value.prefix}#{num}",
          image: "#{value.prefix}image#{num}.jpeg",
          description: "#{value.prefix}description#{num}",
          year: num
        },
        [:title, :image, :description, :year]
      )
      |> Repo.insert!()

    query =
      from p in Person,
        where: p.id in ^value.people

    [director, cast1, cast2, cast3] = Repo.all(query)

    Repo.insert!(%Director{person_id: director_id, movie_id: movie.id})

    {3, _cast} =
      Repo.insert_all(
        Cast,
        [
          %{person_id: cast_id1, movie_id: movie.id},
          %{person_id: cast_id2, movie_id: movie.id},
          %{person_id: cast_id3, movie_id: movie.id}
        ],
        returning: true
      )

    Jason.encode!(%{
      id: movie.id,
      title: movie.title,
      image: movie.image,
      description: movie.description,
      year: movie.year,
      directors: [
        %{
          id: director.id,
          full_name: person_full_name(director),
          image: director.image
        }
      ],
      cast: [
        %{
          id: cast1.id,
          full_name: person_full_name(cast1),
          image: cast1.image
        },
        %{
          id: cast2.id,
          full_name: person_full_name(cast2),
          image: cast2.image
        },
        %{
          id: cast3.id,
          full_name: person_full_name(cast3),
          image: cast3.image
        }
      ]
    })
  end

  def insert_movie_plus(_executor, value) do
    num = Enum.random(0..1_000_000)

    movie =
      %Movie{}
      |> Ecto.Changeset.cast(
        %{
          title: "#{value}#{num}",
          image: "#{value}image#{num}.jpeg",
          description: "#{value}description#{num}",
          year: num,
          directors: [
            %{
              first_name: "#{value}Alice",
              last_name: "#{value}Director",
              image: "#{value}image#{num}.jpeg"
            }
          ],
          cast: [
            %{
              first_name: "#{value}Billie",
              last_name: "#{value}Actor",
              image: "#{value}image#{num + 1}.jpeg"
            },
            %{
              first_name: "#{value}Cameron",
              last_name: "#{value}Actor",
              image: "#{value}image#{num + 2}.jpeg"
            }
          ]
        },
        [:title, :image, :description, :year]
      )
      |> Ecto.Changeset.cast_assoc(:directors,
        with: fn struct, params ->
          Ecto.Changeset.cast(struct, params, [:first_name, :middle_name, :last_name, :image])
        end
      )
      |> Ecto.Changeset.cast_assoc(:cast,
        with: fn struct, params ->
          IO.inspect(params)

          Ecto.Changeset.cast(struct, params, [:first_name, :middle_name, :last_name, :image])
          |> IO.inspect()
        end
      )
      |> Repo.insert!()

    Jason.encode!(%{
      id: movie.id,
      image: movie.image,
      title: movie.title,
      year: movie.year,
      description: movie.description,
      directors:
        Enum.map(movie.directors, fn director ->
          %{
            id: director.id,
            full_name: person_full_name(director),
            image: director.image
          }
        end),
      cast:
        Enum.map(movie.cast, fn cast ->
          %{
            id: cast.id,
            full_name: person_full_name(cast),
            image: cast.image
          }
        end)
    })
  end

  defp person_full_name(%{middle_name: ""} = person) do
    "#{person.first_name} #{person.last_name}"
  end

  defp person_full_name(person) do
    "#{person.first_name} #{person.middle_name} #{person.last_name}"
  end

  defp movie_avg_rating(movie) do
    query =
      from r in Review,
        where: r.movie_id == ^movie.id,
        select: avg(r.rating)

    query
    |> Repo.one!()
    |> Decimal.to_float()
  end
end

defimpl Bench.Implementation, for: Bench.Ecto do
  import Ecto.Query

  alias Bench.Ecto.Repo
  alias Bench.Executor

  alias Bench.Ecto.Schemas.{
    Cast,
    User,
    Movie,
    Director,
    Person
  }

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

  def get_ids(%{concurrency: concurrency}) do
    users = Repo.all(from u in User, select: u.id)
    movies = Repo.all(from m in Movie, select: m.id)
    people = Repo.all(from p in Person, select: p.id)

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

  def setup(_executor, "update_movie") do
    query =
      from m in Movie,
        where: like(m.title, "%---%"),
        update: [
          set: [title: fragment("split_part(?, '---', 1)", m.title)]
        ]

    Repo.update_all(query, [])
  end

  def setup(_executor, "insert_user") do
    query =
      from u in User,
        where: like(u.name, ^"#{@insert_prefix}%")

    Repo.delete_all(query)
  end

  def setup(_executor, query_name)
      when query_name in ~w(insert_movie insert_movie_plus) do
    query =
      from d in Director,
        join: m in assoc(d, :movie),
        where: like(m.image, ^"#{@insert_prefix}%")

    Repo.delete_all(query)

    query =
      from c in Cast,
        join: m in assoc(c, :movie),
        where: like(m.image, ^"#{@insert_prefix}%")

    Repo.delete_all(query)

    query =
      from m in Movie,
        where: like(m.image, ^"#{@insert_prefix}%")

    Repo.delete_all(query)

    query =
      from p in Person,
        where: like(p.image, ^"#{@insert_prefix}%")

    Repo.delete_all(query)
  end

  def setup(_executor, _query_name) do
    :ok
  end

  def cleanup(executor, query_name) do
    setup(executor, query_name)
  end
end
