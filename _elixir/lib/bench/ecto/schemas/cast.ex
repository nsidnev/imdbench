defmodule Bench.Ecto.Schemas.Cast do
  use Ecto.Schema

  schema "casts" do
    belongs_to :person, Bench.Ecto.Schemas.Person
    belongs_to :movie, Bench.Ecto.Schemas.Movie

    field :list_order, :integer, default: nil
  end
end
