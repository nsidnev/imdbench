defprotocol Bench.Implementation do
  def execute(instance, query_name, id)
  def get_ids(instance)
  def setup(instance, query_name)
  def cleanup(instance, query_name)
end
