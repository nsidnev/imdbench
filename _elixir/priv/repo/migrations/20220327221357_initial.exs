defmodule Bench.Ecto.Repo.Migrations.Initial do
  use Ecto.Migration

  def change do
    create table(:movies) do
      add :image, :string, null: false
      add :title, :string, null: false
      add :year, :integer, null: false
      add :description, :string, null: false
    end

    create table(:persons) do
      add :first_name, :string, null: false
      add :middle_name, :string, null: false
      add :last_name, :string, null: false
      add :image, :string, null: false
      add :bio, :string, null: false
    end

    create table(:users) do
      add :name, :string, null: false
      add :image, :string, null: false
    end

    create table(:casts) do
      add :list_order, :integer

      add :person_id, references(:persons)
      add :movie_id, references(:movies)
    end

    create index(:casts, [:movie_id])
    create index(:casts, [:person_id])

    create table(:directors) do
      add :list_order, :integer

      add :person_id, references(:persons)
      add :movie_id, references(:movies)
    end

    create index(:directors, [:movie_id])
    create index(:directors, [:person_id])

    create table(:reviews) do
      add :body, :string, null: false
      add :rating, :integer, null: false
      add :creation_time, :utc_datetime, null: false

      add :author_id, references(:users)
      add :movie_id, references(:movies)
    end

    create index(:reviews, [:author_id])
    create index(:reviews, [:movie_id])
  end
end
