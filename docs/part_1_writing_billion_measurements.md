# Part 1: Writing 1 billion measurements to a file

### The Goal:

- The goal here is to create a file with one billion rows, each row containing a weather station name and a temperature.
- There are baseline temperatures for each station given, and the temperature for each row is the baseline temperature for that station plus a random number between -10 and 10.
- The file is a text file with each line of the format `station_name;temperature`.

### Approaches:

- All tests are run on: Apple M2 Max, 32GB RAM, 12 cores

#### Naive approach

```elixir
{:ok, file} = File.open(@measurements_file, [:append, :utf8])

1..count
|> Stream.map(fn _ ->
    station = Enum.at(stations, :rand.uniform(num_stations - 1))
    ws = WeatherStation.measurement(station)

    content = "#{ws.name};#{ws.temperature}\n"
    IO.write(file, content)
end)
|> Stream.run()

File.close(file)
```

- Simple, straightforward approach. This took `47 seconds` for 1 million rows, although most of the time was spent in the `WeatherStation.measurement` function, which calls `WeatherStations.get_station/1`, which we will optimize in the next steps.

#### Batching writes

```elixir
{:ok, file} = File.open(@measurements_file, [:append, :utf8])

1..count
|> Stream.map(fn _ ->
    station = Enum.at(stations, :rand.uniform(num_stations - 1))
    ws = WeatherStation.measurement(station)
    "#{ws.name};#{ws.temperature}\n"
end)
|> Stream.chunk_every(100)
|> Stream.map(&Enum.join(&1))
|> Stream.each(&IO.write(file, &1))
|> Stream.run()
File.close(file)
```

- this improved the time for a mill rows from `47 seconds` to `30 seconds`

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

- Instead of writing one by one to file, why not pre-calculate the whole file in memory and then write it in one go? We can afford the memory consumption.

```elixir
content =
      1..count
      |> Enum.map(fn _ ->
        station = Enum.at(stations, :rand.uniform(num_stations - 1))
        ws = WeatherStation.measurement(station)

        "#{ws.name};#{ws.temperature}\n"
      end)
      |> Enum.join()

{:ok, file} = File.open(@measurements_file, [:append, :utf8])
IO.write(file, content)
```

- This **did not help**. For 1 mil rows, it took 1.3s (up from 0.9s).

#### Adding concurrency to the pre-calculation (best performance)

- Since the previous approach increased the time it took, I thought maybe adding concurrency to the pre-calculation would help.

```elixir
content =
    1..count
    |> Task.async_stream(
    fn _ ->
        station = Enum.at(stations, :rand.uniform(num_stations - 1))
        ws = WeatherStation.measurement(station)

        "#{ws.name};#{ws.temperature}\n"
    end,
    max_concurrency: System.schedulers_online(),
    ordered: false,
    timeout: :infinity
    )
    |> Stream.map(&elem(&1, 1))
    |> Enum.join()

{:ok, file} = File.open(@measurements_file, [:append, :utf8])
IO.write(file, content)
```

- In this version, I added concurrency using `Task.async_stream`. To my surprise this **increased the time** to 7.3s (!) for 1 mil rows.
- I realized that each of tasks is very lightweight, and the overhead of creating a task is more than the actual work done by the task. So, instead of processing a single row in each task, I decided to process 10k rows in each task:

```elixir
content =
      1..count
      |> Stream.chunk_every(10_000)
      |> Task.async_stream(
        fn arr ->
          arr
          |> Enum.map(fn _ ->
            station = Enum.at(stations, :rand.uniform(num_stations - 1))
            ws = WeatherStation.measurement(station)

            "#{ws.name};#{ws.temperature}\n"
          end)
          |> Enum.join()
        end,
        max_concurrency: System.schedulers_online(),
        ordered: false,
        timeout: :infinity
      )
      |> Stream.map(&elem(&1, 1))
      |> Enum.join()

{:ok, file} = File.open(@measurements_file, [:append, :utf8])
IO.write(file, content)
```

- This reduced the time taken for 1 mil rows to **0.1s**!! 🎉🥳
- For 1 billion rows, this implementation took 205 seconds (3m 25s) 🚀🚀

### Concurrency with less memory usage (balance between memory and performance)

- The previous approach used around 14 GB of memory, which is fine for a billion rows. It's possible to reduce this memory usage while still keeping the performance high. Core idea being to have another layer of chunking above our current chunking. We'll chunk the 1 billion rows into smaller chunks of 10 million rows each, and each of these chunk will stream write into the file. Inside each chunks, we'll have our current code, which is to chunk the rows into 10k each and hold them in memory. This way, we limit the max memory usage.

```elixir
{:ok, file} = File.open(@measurements_file, [:append, :utf8])

1..count
|> Stream.chunk_every(10_000_000)
|> Task.async_stream(
    fn arr ->
    arr
    |> Stream.chunk_every(20_000)
    |> Task.async_stream(
        fn arr ->
        arr
        |> Enum.map(fn _ ->
            station = Enum.at(stations, :rand.uniform(num_stations) - 1)
            ws = WeatherStation.measurement(station)

            "#{ws.name};#{ws.temperature}\n"
        end)
        |> Enum.join()
        end,
        max_concurrency: System.schedulers_online() * 5,
        ordered: false,
        timeout: :infinity
    )
    |> Enum.map(&elem(&1, 1))
    end,
    max_concurrency: System.schedulers_online() * 5,
    ordered: false,
    timeout: :infinity
)
|> Stream.map(&elem(&1, 1))
|> Stream.map(&Enum.join(&1))
|> Stream.each(&IO.write(file, &1))
|> Stream.run()
```

- For 1 million rows, this version takes the same amount as before, 0.1 seconds.
- For 1 billion rows, this version takes 235 seconds (3m 55s), which is a bit slower than the previous version, but uses much less memory.
