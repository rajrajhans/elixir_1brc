defmodule OneBRC.BaselineResultsGenerator do
  @moduledoc """
  Uses a known correct way to generate baseline results for the measurements processor.
  Useful for testing the correctness while working on performance improvements.
  """
  import OneBRC.MeasurementsProcessor
  @count 1_000_000_000
  require Logger

  def process, do: process(@count)

  def process(count) do
    Logger.info("Creating Baseline measurements")

    {time, output} = :timer.tc(fn -> process_(count) end)
    time_s = round(time / 1_000_000 * 10) / 10.0

    Logger.info("Processed #{count} rows in #{time_s} s")

    write_result(output, count)
  end

  def process_(count) do
    file_path = measurements_file(count)
    fs = File.stream!(file_path)

    t1 = System.monotonic_time(:millisecond)

    ets_table = :ets.new(:station_stats, [:set, :public])

    fs
    |> Stream.chunk_every(1000)
    |> Task.async_stream(
      fn val -> Enum.map(val, &parse_row/1) end,
      max_concurrency: System.schedulers_online() * 5,
      ordered: false,
      timeout: :infinity
    )
    |> Stream.flat_map(&elem(&1, 1))
    |> Stream.map(&process_row(&1, ets_table))
    |> Stream.run()

    t2 = System.monotonic_time(:millisecond)

    result =
      :ets.tab2list(ets_table)
      |> Enum.map(fn {key, %{min: min, max: max, sum: sum, count: count}} ->
        mean = (sum / (count * 10.0)) |> round_to_single_decimal()

        {key, %{min: min / 10.0, max: max / 10.0, mean: mean}}
      end)

    Logger.info("Processing data 1: #{t2 - t1} ms")
    Logger.info("Processing data 2: #{System.monotonic_time(:millisecond) - t2} ms")

    result_txt =
      result
      |> Enum.sort_by(fn {key, _} -> key end)
      |> Enum.reduce("", fn {key, %{min: min, max: max, mean: mean}}, acc ->
        acc <> "#{key};#{min};#{mean};#{max}\n"
      end)

    result_txt
  end

  defp parse_row(row) do
    case row do
      "" ->
        nil

      row ->
        [key, value] = :binary.split(row, ";")

        parsed_value =
          value |> String.trim_trailing()

        [a, b] = parsed_value |> :binary.split(".")
        parsed_value = (a <> b) |> String.to_integer()

        [key, parsed_value]
    end
  end

  defp process_row([key, val], ets_table) do
    existing_record = :ets.lookup(ets_table, key)

    new_record =
      case existing_record do
        [] ->
          %{
            min: val,
            max: val,
            sum: val,
            count: 1
          }

        [{^key, record}] ->
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
      end

    :ets.insert(ets_table, {key, new_record})
  end

  defp round_to_single_decimal(number) do
    round(number * 10) / 10.0
  end

  defp write_result(result, count) do
    File.write!(baseline_results_file(count), result)
  end
end
