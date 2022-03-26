defmodule Bench.Ecto.Schemas.Movie do
  use Ecto.Schema

  schema "movies" do
    many_to_many :directors, Bench.Ecto.Schemas.Person, join_through: Bench.Ecto.Schemas.Director
    many_to_many :cast, Bench.Ecto.Schemas.Person, join_through: Bench.Ecto.Schemas.Cast
    has_many :reviews, Bench.Ecto.Schemas.Review

    field :image, :string
    field :title, :string
    field :year, :integer
    field :description, :string
  end
end
