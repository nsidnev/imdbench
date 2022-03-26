defprotocol Bench.Executor do
  def get_user(executor, id)
  def get_movie(executor, id)
  def get_person(executor, id)
  def update_movie(executor, id)
  def insert_user(executor, id)
  def insert_movie(executor, id)
  def insert_movie_plus(executor, id)
end
