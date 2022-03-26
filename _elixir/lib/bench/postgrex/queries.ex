defmodule Bench.Postgrex.Queries do
  @get_user """
  SELECT
      users.id,
      users.name,
      users.image,
      q.review_id,
      q.review_body,
      q.review_rating,
      q.movie_id,
      q.movie_image,
      q.movie_title,
      q.movie_avg_rating
  FROM
      users,
      LATERAL (
          SELECT
              review.id AS review_id,
              review.body AS review_body,
              review.rating AS review_rating,
              movie.id AS movie_id,
              movie.image AS movie_image,
              movie.title AS movie_title,
              movie.avg_rating AS movie_avg_rating
          FROM
              reviews AS review
              INNER JOIN movies AS movie
                  ON (review.movie_id = movie.id)
          WHERE
              review.author_id = users.id
          ORDER BY
              review.creation_time DESC
          LIMIT 10
      ) AS q
  WHERE
      users.id = $1
  """

  @get_person """
  SELECT
      person.id,
      person.full_name,
      person.image,
      person.bio,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
        FROM
          (SELECT
              ROW(
                  movie.id,
                  movie.image,
                  movie.title,
                  movie.year,
                  movie.avg_rating
              ) AS v
          FROM
              actors
              INNER JOIN movies AS movie
                  ON (actors.movie_id = movie.id)
          WHERE
              actors.person_id = person.id
          ORDER BY
              movie.year ASC, movie.title ASC
          ) AS q
      ) AS acted_in,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
        FROM
          (SELECT
              ROW(
                  movie.id,
                  movie.image,
                  movie.title,
                  movie.year,
                  movie.avg_rating
              ) AS v
          FROM
              directors
              INNER JOIN movies AS movie
                  ON (directors.movie_id = movie.id)
          WHERE
              directors.person_id = person.id
          ORDER BY
              movie.year ASC, movie.title ASC
          ) AS q
      ) AS directed
  FROM
      persons AS person
  WHERE
      id = $1;
  """

  @get_movie """
  SELECT
      movie.id,
      movie.image,
      movie.title,
      movie.year,
      movie.description,
      movie.avg_rating,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
        FROM
          (SELECT
              ROW(
                  person.id,
                  person.full_name,
                  person.image
              ) AS v
          FROM
              directors
              INNER JOIN persons AS person
                  ON (directors.person_id = person.id)
          WHERE
              directors.movie_id = movie.id
          ORDER BY
              directors.list_order NULLS LAST,
              person.last_name
          ) AS q
      ) AS directors,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
        FROM
          (SELECT
              ROW(
                  person.id,
                  person.full_name,
                  person.image
              ) AS v
          FROM
              actors
              INNER JOIN persons AS person
                  ON (actors.person_id = person.id)
          WHERE
              actors.movie_id = movie.id
          ORDER BY
              actors.list_order NULLS LAST,
              person.last_name
          ) AS q
      ) AS actors,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
        FROM
          (SELECT
              ROW(
                  review.id,
                  review.body,
                  review.rating,
                  (SELECT
                      ROW(
                          author.id,
                          author.name,
                          author.image
                      )
                      FROM
                          users AS author
                      WHERE
                          review.author_id = author.id
                  )
              ) AS v
          FROM
              reviews AS review
          WHERE
              review.movie_id = movie.id
          ORDER BY
              review.creation_time DESC
          ) AS q
      ) AS reviews
  FROM
      movies AS movie
  WHERE
      id = $1;
  """

  @update_movie """
  UPDATE
    movies
  SET
    title = movies.title || $2
  WHERE
    movies.id = $1
  RETURNING
    movies.id, movies.title
  """

  @insert_user """
  INSERT INTO users (name, image) VALUES
    ($1, $2)
  RETURNING
    users.id, users.name, users.image
  """

  @insert_movie """
  WITH
  m AS (
      INSERT INTO movies AS M (title, image, description, year)
      VALUES ($1, $2, $3, $4)
      RETURNING M.id, M.title, M.image, M.description, M.year
  ),
  d AS (
      SELECT
          person.id,
          person.full_name,
          person.image
      FROM
          persons AS person
      WHERE
          id = $5
  ),
  c AS (
      SELECT
          person.id,
          person.full_name,
          person.image
      FROM
          persons AS person
      WHERE
          id IN ($6, $7, $8)
  ),
  dl AS (
      INSERT INTO directors (person_id, movie_id)
      (SELECT d.id, m.id FROM m, d)
  ),
  cl AS (
      INSERT INTO actors (person_id, movie_id)
      (SELECT c.id, m.id FROM m, c)
  )
  SELECT
      m.id,
      m.image,
      m.title,
      m.year,
      m.description,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
      FROM
          (SELECT
              ROW(id, full_name, image) AS v
          FROM
              d
          ) AS q
      ) AS directors,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
      FROM
          (SELECT
              ROW(id, full_name, image) AS v
          FROM
              c
          ) AS q
      ) AS actors
  FROM
      m
  """

  @insert_movie_plus """
  WITH
  m AS (
      INSERT INTO movies AS M (title, image, description, year)
      VALUES ($1, $2, $3, $4)
      RETURNING M.id, M.title, M.image, M.description, M.year
  ),
  p AS (
      INSERT INTO persons AS P (first_name, last_name, image, bio)
      VALUES
          ($5, $6, $7, ''),
          ($8, $9, $10, ''),
          ($11, $12, $13, '')
      RETURNING
          P.id, P.last_name, P.full_name, P.image
  ),
  dl AS (
      INSERT INTO directors (person_id, movie_id)
      (SELECT p.id, m.id FROM m, p WHERE p.last_name = 'Director')
  ),
  cl AS (
      INSERT INTO actors (person_id, movie_id)
      (SELECT p.id, m.id FROM m, p WHERE p.last_name = 'Actor')
  )
  SELECT
      m.id,
      m.image,
      m.title,
      m.year,
      m.description,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
      FROM
          (SELECT
              ROW(id, full_name, image) AS v
          FROM
              p
          WHERE
              p.last_name = 'Director'
          ) AS q
      ) AS directors,
      (SELECT
          COALESCE(array_agg(q.v), (ARRAY[])::record[])
      FROM
          (SELECT
              ROW(id, full_name, image) AS v
          FROM
              p
          WHERE
              p.last_name = 'Actor'
          ) AS q
      ) AS actors
  FROM
      m
  """

  def get_user, do: @get_user
  def get_person, do: @get_person
  def get_movie, do: @get_movie
  def update_movie, do: @update_movie
  def insert_user, do: @insert_user
  def insert_movie, do: @insert_movie
  def insert_movie_plus, do: @insert_movie_plus
end
