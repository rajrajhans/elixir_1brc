defmodule OneBRC.MeasurementsProcessor do
  @measurements_file "./data/measurements.{COUNT}.txt"
  @count 1_000_000_000
  @latest_version "N"

  require Logger

  def process, do: process(@count, @latest_version)

  def process(count, version) do
    Logger.info("Processing #{count} measurements with version #{version}")

    {time, {output, t1, t2, t3}} = :timer.tc(fn -> process_with_version(count, version) end)
    time_s = round(time / 1_000_000 * 10) / 10.0

    Logger.info("Processing data, stage 1 took: #{t2 - t1} ms")
    Logger.info("Processing data, stage 2 took: #{t3 - t2} ms")
    Logger.info("Processed #{count} rows in #{time_s} s")

    write_result(output, count)
    verify_result(count)
  end

  defp process_with_version(count, version) do
    case String.upcase(version) do
      "N" -> OneBRC.MeasurementsProcessor.VersionN.process(count)
      _ -> raise "Unknown version"
    end
  end

  def measurements_file(count) do
    String.replace(@measurements_file, "{COUNT}", Integer.to_string(count))
  end

  def results_file(count) do
    "./data/result.#{count}.json"
  end

  def baseline_results_file(count) do
    "./data/result_baseline.#{count}.json"
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

    File.write!(results_file(count), Jason.encode!(map))
  end

  defp verify_result(count) do
    # optional correctness check
    baseline_file_path = baseline_results_file(count)

    if File.exists?(baseline_file_path) do
      is_correct =
        OneBRC.Utilities.CompareData.compare_json(baseline_file_path, results_file(count))

      if is_correct do
        Logger.info("Result is correct")
      else
        Logger.error("Result is incorrect")
        raise "Result is incorrect"
      end
    end
  end
end
