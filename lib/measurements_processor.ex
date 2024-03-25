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

    write_result(output, count)
  end

  def process_(count) do
    file_path = measurements_file(count)
    {:ok, content} = File.read(file_path)

    t1 = System.monotonic_time(:millisecond)

    result =
      content
      |> String.split("\n")
      |> Enum.map(&String.split(&1, ";"))
      |> Enum.reject(fn value -> value |> Enum.at(0) == "" end)
      |> Enum.reduce(%{}, fn [key, value], acc ->
        {val, _} = Float.parse(value)

        default = %{
          min: val,
          max: val,
          sum: val,
          count: 1
        }

        Map.update(acc, key, default, fn %{
                                           min: min,
                                           max: max,
                                           sum: sum,
                                           count: count
                                         } ->
          min = if val < min, do: val, else: min
          max = if val > max, do: val, else: max
          sum = sum + val
          count = count + 1

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

    Logger.info("Processing data 1: #{t2 - t1} ms")
    Logger.info("Processing data 2: #{System.monotonic_time(:millisecond) - t2} ms")

    result
  end

  defp measurements_file(count) do
    String.replace(@measurements_file, "{COUNT}", Integer.to_string(count))
  end

  defp round_to_single_decimal(number) do
    round(number * 10) / 10.0
  end

  defp write_result(result, count) do
    map =
      result
      |> Enum.reduce([], fn {key, values}, acc ->
        acc ++
          [
            %{
              station_name: key,
              min: values.min,
              max: values.max,
              mean: values.mean
            }
          ]
      end)

    File.write!("./data/result_b1.#{count}.json", Jason.encode!(map))
  end
end
