defmodule OneBRC.MeasurementsProcessor.Version2 do
  @moduledoc """
  diff from version N: post-aggregation processing is done in a single pass after list concat and flattening, instead of 2 level deep reduce
  """
  import OneBRC.MeasurementsProcessor

  require Logger

  def process(count) do
    t1 = System.monotonic_time(:millisecond)
    file_path = measurements_file(count)
    fs = File.stream!(file_path)

    ets_table = :ets.new(:station_stats, [:set, :public])

    fs
    |> Stream.chunk_every(10000)
    |> Task.async_stream(
      fn val -> Enum.map(val, &parse_row/1) end,
      max_concurrency: System.schedulers_online(),
      ordered: false,
      timeout: :infinity
    )
    |> Stream.with_index()
    |> Task.async_stream(
      fn {{:ok, parsed_rows}, row_index} ->
        interim_records =
          Enum.reduce(parsed_rows, %{}, fn row, acc ->
            process_row(row, acc)
          end)

        :ets.insert(ets_table, {row_index, interim_records})
      end,
      max_concurrency: System.schedulers_online(),
      ordered: false,
      timeout: :infinity
    )
    |> Stream.run()

    t2 = System.monotonic_time(:millisecond)

    result =
      :ets.tab2list(ets_table)
      |> Enum.reduce([], fn {_, m}, acc -> [acc | Enum.into(m, [])] end)
      |> List.flatten()
      |> Enum.reduce(%{}, fn {key, val}, acc ->
        existing_record = Map.get(acc, key, nil)

        new_record =
          case existing_record do
            nil ->
              val

            %{count: count, min: min, max: max, mean: mean} ->
              min = if val.min < min, do: val.min, else: min
              max = if val.max > max, do: val.max, else: max
              new_c = count + val.count

              mean = (mean * count + val.mean * val.count) / new_c

              %{
                min: min,
                max: max,
                mean: mean,
                count: new_c
              }
          end

        Map.put(acc, key, new_record)
      end)
      |> Enum.map(fn {key, value} ->
        # bring it back to floating point
        {key,
         %{
           min: round_to_single_decimal(value.min / 10.0),
           max: round_to_single_decimal(value.max / 10.0),
           mean: round_to_single_decimal(value.mean / 10.0)
         }}
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

  defp process_row([key, val], acc) do
    existing_record = Map.get(acc, key, nil)

    new_record =
      case existing_record do
        nil ->
          %{
            min: val,
            max: val,
            mean: val,
            count: 1
          }

        %{count: count, min: min, max: max, mean: mean} ->
          min = if val < min, do: val, else: min
          max = if val > max, do: val, else: max
          new_c = count + 1

          mean = (mean * count + val) / new_c

          %{
            min: min,
            max: max,
            mean: mean,
            count: new_c
          }
      end

    Map.put(acc, key, new_record)
  end
end
