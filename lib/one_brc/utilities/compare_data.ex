defmodule OneBRC.Utilities.CompareData do
  def compare_json(baseline, new) do
    baseline = Jason.decode!(File.read!(baseline))
    new = Jason.decode!(File.read!(new))

    Enum.all?(baseline, fn record ->
      station_name = Map.get(record, "station_name")
      baseline_mean = Map.get(record, "mean")
      baseline_max = Map.get(record, "max")
      baseline_min = Map.get(record, "min")

      new_record =
        Enum.find(new, fn record -> Map.get(record, "station_name") == station_name end)

      new_mean = Map.get(new_record, "mean")
      new_max = Map.get(new_record, "max")
      new_min = Map.get(new_record, "min")

      is_mean_correct = baseline_mean == new_mean
      is_max_correct = baseline_max == new_max
      is_min_correct = baseline_min == new_min

      if Enum.any?([is_mean_correct, is_max_correct, is_min_correct], fn x -> x == false end) do
        IO.puts("Error: #{station_name}")
        IO.puts("Baseline: #{baseline_mean}, #{baseline_max}, #{baseline_min}")
        IO.puts("New: #{new_mean}, #{new_max}, #{new_min}")
      end

      is_mean_correct and is_max_correct and is_min_correct
    end)
  end
end
