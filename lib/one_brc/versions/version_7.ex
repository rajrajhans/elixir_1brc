defmodule OneBRC.MeasurementsProcessor.Version7.Worker do
  def run(parent_pid) do
    ask_for_work(parent_pid)

    receive do
      {:do_work, chunk} ->
        process_chunk(chunk)
        run(parent_pid)

      :result ->
        send(parent_pid, {:result, :erlang.get()})
        # die
    end
  end

  defp ask_for_work(parent_pid) do
    send(parent_pid, {:give_work, self()})
  end

  defp process_chunk(bin) do
    :binary.split(bin, "\n", [:global])
    |> Enum.map(&parse_row/1)
    |> Enum.map(fn row ->
      process_row(row)
    end)
  end

  defp parse_row("") do
    nil
  end

  defp parse_row(row) do
    [key, t_value] = :binary.split(row, ";")
    parsed_temp = t_value |> parse_temperature()
    [key, parsed_temp]
  end

  # ex: -4.5
  defp parse_temperature(<<?-, d1, ?., d2, _::binary>>) do
    -(char_to_num(d1) * 10 + char_to_num(d2))
  end

  # ex: 4.5
  defp parse_temperature(<<d1, ?., d2, _::binary>>) do
    char_to_num(d1) * 10 + char_to_num(d2)
  end

  # ex: -45.3
  defp parse_temperature(<<?-, d1, d2, ?., d3, _::binary>>) do
    -(char_to_num(d1) * 100 + char_to_num(d2) * 10 + char_to_num(d3))
  end

  # ex: 45.3
  defp parse_temperature(<<d1, d2, ?., d3, _::binary>>) do
    char_to_num(d1) * 100 + char_to_num(d2) * 10 + char_to_num(d3)
  end

  defp char_to_num(char) do
    char - ?0
  end

  defp process_row(nil) do
    nil
  end

  defp process_row([key, val]) do
    existing_record = :erlang.get(key)

    new_record =
      case existing_record do
        :undefined ->
          %{
            min: val,
            max: val,
            sum: val,
            count: 1
          }

        %{count: count, min: min, max: max, sum: sum} ->
          min = if val < min, do: val, else: min
          max = if val > max, do: val, else: max
          new_c = count + 1
          new_sum = sum + val

          %{
            min: min,
            max: max,
            sum: new_sum,
            count: new_c
          }
      end

    :erlang.put(key, new_record)
  end
end

defmodule OneBRC.MeasurementsProcessor.Version7 do
  @moduledoc """
  diff from version 6:
  todo

  Performance:
  """
  import OneBRC.MeasurementsProcessor
  alias OneBRC.MeasurementsProcessor.Version7.Worker

  require Logger

  def process(count) do
    t1 = System.monotonic_time(:millisecond)
    file_path = measurements_file(count)

    {:ok, file} = :prim_file.open(file_path, [:raw, :binary, :read])
    worker_count = System.schedulers_online() * 2
    # boot up workers
    wpids =
      Enum.map(1..worker_count, fn _ ->
        spawn_link(Worker, :run, [self()])
      end)

    :ok = read_and_process(file)

    # wait for all workers to finish
    Enum.map(1..worker_count, fn _ ->
      receive do
        {:give_work, _worker_pid} ->
          :ok
      end
    end)

    results =
      wpids
      |> Enum.map(fn wpid ->
        send(wpid, :result)
      end)
      |> Enum.map(fn _ ->
        receive do
          {:result, result} ->
            result
        end
      end)

    :prim_file.close(file)

    t2 = System.monotonic_time(:millisecond)

    result =
      results
      |> List.flatten()
      |> Enum.reduce(%{}, fn {key, val}, acc ->
        existing_record = Map.get(acc, key, nil)

        new_record =
          case existing_record do
            nil ->
              val

            %{count: count, min: min, max: max, sum: sum} ->
              min = if val.min < min, do: val.min, else: min
              max = if val.max > max, do: val.max, else: max
              new_c = count + val.count

              sum = sum + val.sum

              %{
                min: min,
                max: max,
                sum: sum,
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
           mean: round_to_single_decimal(value.sum / value.count / 10.0)
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

  defp read_and_process(file) do
    chunk_size = 1024 * 1024 * 1

    data =
      case :prim_file.read(file, chunk_size) do
        :eof ->
          nil

        {:ok, data} ->
          case :prim_file.read_line(file) do
            {:ok, line} ->
              <<data::binary, line::binary>>

            :eof ->
              data
          end
      end

    if !is_nil(data) do
      receive do
        {:give_work, worker_pid} ->
          send(worker_pid, {:do_work, data})
      end

      read_and_process(file)
    else
      :ok
    end
  end

  defp round_to_single_decimal(number) do
    round(number * 10) / 10.0
  end
end
