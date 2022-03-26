defmodule Bench.Ecto.Schemas.Review do
  use Ecto.Schema

  schema "reviews" do
    belongs_to :author, Bench.Ecto.Schemas.User
    belongs_to :movie, Bench.Ecto.Schemas.Movie

    field :body, :string
    field :rating, :integer
    field :creation_time, :utc_datetime
  end
end
