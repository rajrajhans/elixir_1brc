defmodule OneBRC.MeasurementsProcessor.Version2 do
  import OneBRC.MeasurementsProcessor
  require Logger

  def process(count) do
    file_path = measurements_file(count)
    fs = File.stream!(file_path)

    t1 = System.monotonic_time(:millisecond)

    result =
      fs
      |> Stream.map(&String.split(&1, ";"))
      |> Stream.reject(fn value -> value |> Enum.at(0) == "" end)
      |> Enum.reduce(%{}, fn [key, value], acc ->
        {val, _} = Float.parse(value)

        default = %{
          min: val,
          max: val,
          sum: val,
          count: 1
        }

        Map.update(acc, key, default, fn record ->
          min = if val < record.min, do: val, else: record.min
          max = if val > record.max, do: val, else: record.max
          sum = record.sum + val
          count = record.count + 1

          %{
            min: min,
            max: max,
            sum: sum,
            count: count
          }
        end)
      end)

    t2 = System.monotonic_time(:millisecond)

    result =
      result
      |> Enum.map(fn {key, %{min: min, max: max, sum: sum, count: count}} ->
        mean = (sum / count) |> round_to_single_decimal()

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
