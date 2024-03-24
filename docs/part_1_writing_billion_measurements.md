# Part 1: Writing 1 billion measurements to a file

### The Goal:

- The goal here is to create a file with one billion rows, each row containing a weather station name and a temperature.
- There are baseline temperatures for each station given, and the temperature for each row is the baseline temperature for that station plus a random number between -10 and 10.
- The file is a text file with each line of the format `station_name;temperature`.

### Approaches:

- All tests are run on: Apple M2 Max, 32GB RAM, 12 cores

#### Baseline

Let's see how long it takes to write one billion rows of the same data to a file, using `awk`

```sh
time awk 'BEGIN { for (i=0; i<1000000000; i++) print "station_name;10" }' > data.txt
```

- took `2:18.61 total`

#### Naive approach

#### Batching writes

#### Opening file in raw mode, buffered writes

- **No improvements** seen

### Optimizing `WeatherStations.get_station/1`

```elixir
 def get_station(name) do
    stations = stations_data()
    temp = Map.fetch!(stations, name)
    %WeatherStation{name: name, temperature: temp}
  end

  def stations_data do
    Enum.into(
      [
.... 400 items
     ], %{})
```

- I realized that `WeatherStations.get_station/1` function was getting called while writing each row. Each call is doing a `Enum.into`, which converts the list into a map.
- To fix this, I created another module, and set the stations map as a module attribute.

```elixir
defmodule OneBRC.Measurements.Data do
  @stations_data %{
    .... 400 items
  }
  def stations_data, do: @stations_data
end
```

- After this change, the time taken for 1 million rows went from 29 seconds to 0.9 seconds!
- I also tried creating an agent to store the stations data at start, and then using `Agent.get` to get the data. But it was slower than the module attribute approach. Can't beat compile-time optimizations!

#### Pre-calculating the whole file in memory, and then writing it in single shot

#### Adding concurrency to the pre-calculation

- weird behaviour with one billion rows

```

```
