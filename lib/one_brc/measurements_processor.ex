defmodule OneBRC.MeasurementsProcessor do
  @measurements_file "./data/measurements.{COUNT}.txt"
  @count 1_000_000_000
  @latest_version "N"

  require Logger

  def process, do: process(@count, @latest_version)

  def process(count, version) do
    Logger.info("Processing #{count} measurements with version #{version}")

    {time, output} = :timer.tc(fn -> process_with_version(count, version) end)
    time_s = Float.round(time / 1_000_000, 3)

    Logger.info("Processed #{count} rows in #{time_s} s")

    write_result(output, count)
    verify_result(count)
  end

  defp process_with_version(count, version) do
    case String.upcase(version) do
      "N" -> OneBRC.MeasurementsProcessor.Version8.process(count)
      "1" -> OneBRC.MeasurementsProcessor.Version1.process(count)
      "2" -> OneBRC.MeasurementsProcessor.Version2.process(count)
      "3" -> OneBRC.MeasurementsProcessor.Version3.process(count)
      "4" -> OneBRC.MeasurementsProcessor.Version4.process(count)
      "5" -> OneBRC.MeasurementsProcessor.Version5.process(count)
      "6" -> OneBRC.MeasurementsProcessor.Version6.process(count)
      "7" -> OneBRC.MeasurementsProcessor.Version7.process(count)
      "8" -> OneBRC.MeasurementsProcessor.Version8.process(count)
      _ -> raise "Unknown version"
    end
  end

  def measurements_file(count) do
    String.replace(@measurements_file, "{COUNT}", Integer.to_string(count))
  end

  def results_file(count) do
    "./data/result.#{count}.txt"
  end

  def baseline_results_file(count) do
    "./data/result_baseline.#{count}.txt"
  end

  defp write_result(result, count) do
    File.write!(results_file(count), result)
  end

  defp verify_result(count) do
    # optional correctness check
    should_skip_verification = System.get_env("SKIP_RESULT_VERIFICATION", "false")

    if should_skip_verification == "true" do
      Logger.info("Skipping correctness check")
    else
      baseline_file_path = baseline_results_file(count)
      result_file_path = results_file(count)

      if File.exists?(baseline_file_path) do
        is_correct = OneBrc.ResultVerification.verify_result(result_file_path, baseline_file_path)

        if is_correct do
          Logger.info("Result is correct")
        else
          Logger.error("Result is incorrect")
          raise "Result is incorrect"
        end
      else
        Logger.error("Baseline file not found. Skipping correctness check")
      end
    end
  end
end
