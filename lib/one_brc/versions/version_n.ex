defmodule OneBRC.MeasurementsProcessor.VersionN do
  import OneBRC.MeasurementsProcessor

  require Logger

  def process(count) do
    file_path = measurements_file(count)
    fs = File.stream!(file_path)

    t1 = System.monotonic_time(:millisecond)

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
      |> Enum.reduce(%{}, fn {_row_idx, interim_records_map}, super_acc ->
        aggregated_records_for_current_row =
          Enum.reduce(interim_records_map, %{}, fn {key, interim_record}, temp_acc ->
            existing_aggregated_record = Map.get(super_acc, key, nil)

            new_aggregated_record =
              case existing_aggregated_record do
                nil ->
                  interim_record

                aggregated_record ->
                  min = :erlang.min(interim_record.min, aggregated_record.min)
                  max = :erlang.max(interim_record.max, aggregated_record.max)
                  count = aggregated_record.count + interim_record.count

                  mean =
                    (aggregated_record.mean * aggregated_record.count +
                       interim_record.mean * interim_record.count) / count

                  %{
                    min: min,
                    max: max,
                    mean: mean,
                    count: count
                  }
              end

            Map.put(temp_acc, key, new_aggregated_record)
          end)

        :maps.merge(super_acc, aggregated_records_for_current_row)
      end)
      |> Enum.map(fn {key, value} ->
        mean = (value.mean / 10.0) |> round_to_single_decimal()

        {key,
         %{
           min: round_to_single_decimal(value.min / 10.0),
           max: round_to_single_decimal(value.max / 10.0),
           mean: mean
         }}
      end)

    t3 = System.monotonic_time(:millisecond)
    Logger.info("Processing data, stage 1 took: #{t2 - t1} ms")
    Logger.info("Processing data, stage 2 took: #{t3 - t2} ms")

    result_txt =
      result
      |> Enum.sort_by(fn {key, _} -> key end)
      |> Enum.reduce("", fn {key, %{min: min, max: max, mean: mean}}, acc ->
        acc <> "#{key};#{min};#{mean};#{max}\n"
      end)

    t4 = System.monotonic_time(:millisecond)

    Logger.info("Processing data, stage 3 took: #{t4 - t3} ms")

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

          mean = (mean * count + val) / (count + 1)

          %{
            min: min,
            max: max,
            mean: mean,
            count: count + 1
          }
      end

    Map.put(acc, key, new_record)
  end
end
