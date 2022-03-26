defmodule Bench.Ecto.Schemas.Person do
  use Ecto.Schema

  schema "persons" do
    many_to_many :directed, Bench.Ecto.Schemas.Movie, join_through: Bench.Ecto.Schemas.Director
    many_to_many :acted_in, Bench.Ecto.Schemas.Movie, join_through: Bench.Ecto.Schemas.Cast

    field :first_name, :string
    field :middle_name, :string, null: false, default: ""
    field :last_name, :string
    field :image, :string
    field :bio, :string, null: false, default: ""
  end
end
