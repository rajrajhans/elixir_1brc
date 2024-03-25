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

      baseline_mean == new_mean and baseline_max == new_max and baseline_min == new_min
    end)
  end
end
