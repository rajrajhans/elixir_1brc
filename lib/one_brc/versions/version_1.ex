defmodule OneBRC.MeasurementsProcessor.Version1 do
  import OneBRC.MeasurementsProcessor
  require Logger

  def process(count) do
    t1 = System.monotonic_time(:millisecond)
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

    t2 = System.monotonic_time(:millisecond)

    result =
      acc
      |> Enum.map(fn {key, values} ->
        min = Enum.min(values) |> round_to_single_decimal()
        max = Enum.max(values) |> round_to_single_decimal()
        mean = (Enum.sum(values) / length(values)) |> round_to_single_decimal()

        {key, %{min: min, max: max, mean: mean}}
      end)

    t3 = System.monotonic_time(:millisecond)

    result_txt =
      result
      |> Enum.sort_by(fn {key, _} -> key end)
      |> Enum.reduce("", fn {key, %{min: min, max: max, mean: mean}}, acc ->
        acc <> "#{key};#{min};#{mean};#{max}\n"
      end)

    t4 = System.monotonic_time(:millisecond)

    Logger.info("Processing data, stage 1 (processing) took: #{t2 - t1} ms")
    Logger.info("Processing data, stage 2 (aggregating) took: #{t3 - t2} ms")
    Logger.info("Processing data, stage 3 (sorting & txt creation) took: #{t4 - t3} ms")

    result_txt
  end

  defp round_to_single_decimal(number) do
    round(number * 10) / 10.0
  end
end
