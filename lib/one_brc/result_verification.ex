defmodule OneBrc.ResultVerification do
  require Logger

  def verify_result(generated_file, baseline_file) do
    generated_data = read_and_parse_file(generated_file)
    baseline_data = read_and_parse_file(baseline_file)

    sorting_correct = check_sorting(generated_data)
    content_differences = compare_data(generated_data, baseline_data)

    case {sorting_correct, content_differences} do
      {true, []} ->
        true

      {false, []} ->
        Logger.error("Generated file is not sorted correctly.")
        false

      {_, differences} ->
        Enum.each(differences, &print_difference/1)
        false
    end
  end

  defp read_and_parse_file(file_path) do
    file_path
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.map(&parse_line/1)
  end

  defp parse_line(line) do
    [station, min, mean, max] = String.split(line, ";")
    {station, {parse_float(min), parse_float(mean), parse_float(max)}}
  end

  defp parse_float(str), do: String.to_float(str)

  defp check_sorting(data) do
    stations = Enum.map(data, fn {station, _} -> station end)
    stations == Enum.sort(stations)
  end

  defp compare_data(generated, baseline) do
    generated_map = Enum.into(generated, %{})
    baseline_map = Enum.into(baseline, %{})

    keys = MapSet.new(Map.keys(generated_map) ++ Map.keys(baseline_map))

    Enum.reduce(Enum.sort(keys), [], fn station, acc ->
      case {Map.get(generated_map, station), Map.get(baseline_map, station)} do
        {nil, _} ->
          [{:missing_in_generated, station} | acc]

        {_, nil} ->
          [{:missing_in_baseline, station} | acc]

        {gen_value, base_value} when gen_value != base_value ->
          [{:value_mismatch, station, gen_value, base_value} | acc]

        _ ->
          acc
      end
    end)
  end

  defp print_difference({:missing_in_generated, station}) do
    Logger.error("Station #{station} is missing in the generated file.")
  end

  defp print_difference({:missing_in_baseline, station}) do
    Logger.error("Station #{station} is missing in the baseline file.")
  end

  defp print_difference({:value_mismatch, station, gen_value, base_value}) do
    Logger.error("Mismatch for station #{station}:")
    Logger.error("  Generated: #{format_value(gen_value)}")
    Logger.error("  Baseline:  #{format_value(base_value)}")
  end

  defp format_value({min, mean, max}) do
    "#{min};#{mean};#{max}"
  end
end
