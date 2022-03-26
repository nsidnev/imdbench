defmodule Bench do
  def parse_args(args) do
    {parsed, _args} =
      OptionParser.parse!(args,
        strict: [
          concurrency: :integer,
          duration: :integer,
          timeout: :integer,
          warmup_time: :integer,
          output_format: :string,
          host: :string,
          port: :integer,
          user: :string,
          nsamples: :integer,
          number_of_ids: :integer,
          query: :string
        ]
      )

    parsed
  end
end
