defmodule OneBRC.MeasurementsProcessor do
  @measurements_file "./data/measurements.{COUNT}.txt"
  @count 1_000_000_000
  require Logger

  def process, do: process(@count)

  def process(count) do
    Logger.info("Processing measurements")

    {time, output} = :timer.tc(fn -> process_(count) end)
    time_s = round(time / 1_000_000 * 10) / 10.0

    Logger.info("***OUTPUT***:\n\n#{inspect(output)}\n\n")
    Logger.info("Processed #{count} rows in #{time_s} s")
  end

  def process_(count) do
    file_path = measurements_file(count)
    {:ok, content} = File.read(file_path)

    acc =
      content
      |> String.split("\n")
      |> Enum.map(&String.split(&1, ";"))
      |> Enum.reject(fn value -> value |> Enum.at(0) == "" end)
      |> Enum.reduce(%{}, fn [key, value], acc ->
        {val, _} = Float.parse(value)
        Map.update(acc, key, [val], fn v -> [val | v] end)
      end)

    result =
      acc
      |> Enum.map(fn {key, values} ->
        min = Enum.min(values) |> round_to_single_decimal()
        max = Enum.max(values) |> round_to_single_decimal()
        mean = (Enum.sum(values) / length(values)) |> round_to_single_decimal()

        {key, %{min: min, max: max, mean: mean}}
      end)

    result
  end

  defp measurements_file(count) do
    String.replace(@measurements_file, "{COUNT}", Integer.to_string(count))
  end

  defp round_to_single_decimal(number) do
    round(number * 10) / 10.0
  end
end
