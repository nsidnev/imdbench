defmodule Bench.Ecto.Schemas.User do
  use Ecto.Schema

  schema "users" do
    has_many :reviews, Bench.Ecto.Schemas.Review, foreign_key: :author_id

    field :name, :string
    field :image, :string
  end
end
