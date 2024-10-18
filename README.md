## The One Billion Row Challenge (in Elixir)

1BRC is a challenge, [originally in Java](https://github.com/gunnarmorling/1brc) to process a text file of weather station names and temperatures, and for each weather station, print out the minimum, mean, and maximum. It sounds simple, but the catch is that the file contains one billion rows. Fun!

![1BRC Title Slide](https://assets.rajrajhans.com/elixir-1brc-perf-traces/1brc_title_slide.jpeg)

> A talk related to this was presented at [Code Beam Europe 2024](https://codebeameurope.com/talks/the-one-billion-row-challenge-in-elixir-from-12-minutes-to-25-seconds/). The [slides are available here](https://assets.rajrajhans.com/elixir-1brc-perf-traces/raj_code_beam_2024_compressed.pdf).

## Setting up this repo

- This repo uses Nix flakes to manage dependencies. To get started, run `direnv allow` in the root of the repo to activate the Nix environment.
- Execute `run deps` to install dependencies.
- To create measurements file, execute `run create_measurements 1_000_000`. This will create a file `data/measurements.1000000.txt` with 1 million measurements.
- To process the measurements, execute `run process_measurements 1_000_000`. This will process the file created in the previous step.
- That's it, you can follow the codepaths, starting from `bin/run` to explore further ✌️

## Available commands

- `run deps` - installs mix dependencies
- `run iex` - spawns an iex session
- `run format` - runs mix format
- `run process_measurements --count=x --version=y` - runs the code for version y with the measurement file that has x number of measurements. also verifies that the results are correct by comparing with baseline results
- `run create_measurements <count>` - creates a measurements file with specified count
- `run create_measurements.profile <count>` - creates measurements file with profiling
- `run create_baseline_results <count>` - generates baseline results with a known correct way
- `run process_measurements.repeat --count=x --version=y` - runs process_measurements 5 times with given count and version
- `run_with_cpu_profiling <command>` - runs a command with CPU profiling
- `run livebook.setup` - sets up Livebook
- `run livebook.server` - starts Livebook server
- `run livebook` - starts both iex and Livebook server concurrently
- `run process_measurements.profile.eprof --count=x --version=y` - profiles with eprof
- `run process_measurements.profile.cprof --count=x --version=y` - profiles with cprof
- `run process_measurements.profile.eflambe --count=x --version=y` - profiles with eflambe
- `run process_measurements.profile.benchee` - runs [script](./benchee.exs) for using benchee to compare different versions
- `run all_versions --count=x` - runs all versions with x measurements

## Writing measurements to a file

- The goal here is to create a file with one billion rows, each row containing a weather station name and a temperature.
- There are baseline temperatures for each station given, and the temperature for each row is the baseline temperature for that station plus a random number between -10 and 10.
- The file is a text file with each line of the format `station_name;temperature`.
- The fastest solution I've implemented so far creates the file with 1 billion measurements in 205 seconds. The code is at [`lib/one_brc/measurements_generator.ex`](./lib/one_brc/measurements_generator.ex).

### Commands for creating measurements:

- `run create_measurements 1000` - creates a file `./data/measurements.1000.txt` with 1000 measurements. You can change 1000 to the number of measurements you want.
- `run create_measurements.profile 1000` - creates a file `./data/measurements.1000.txt` with 1000 measurements and profiles the execution using `eprof`.
- Check `./bin/run` to see what the above commands do.

## Performance Traces of different versions

- I've used [eFlambe](https://github.com/Stratus3D/eflambe) to get performance traces of different versions. Following are the links to the traces, viewed in Speedoscope:

- [Speedoscope: Version 1, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-1-count-1000-eflambe-output.bggg&title=ex-1brc-v1), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-1-count-1000-eflambe-output.bggg)
- [Speedoscope: Version 2, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-2-count-1000-eflambe-output.bggg&title=ex-1brc-v2), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-2-count-1000-eflambe-output.bggg&title=ex-1brc-v2)
- [Speedoscope: Version 3, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-3-count-1000-eflambe-output.bggg&title=ex-1brc-v3), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-3-count-1000-eflambe-output.bggg)
- [Speedoscope: Version 4, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-4-count-1000-eflambe-output.bggg&title=ex-1brc-v4), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-4-count-1000-eflambe-output.bggg)
- [Speedoscope: Version 5, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-5-count-1000-eflambe-output.bggg&title=ex-1brc-v5), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-5-count-1000-eflambe-output.bggg)
- [Speedoscope: Version 6, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-6-count-1000-eflambe-output.bggg&title=ex-1brc-v6), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-6-count-1000-eflambe-output.bggg)
- [Speedoscope: Version 7, 1000 measurements](https://www.speedscope.app/#profileURL=https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-7-count-1000-eflambe-output.bggg&title=ex-1brc-v7), [Link to bggg file](https://assets.rajrajhans.com/elixir-1brc-perf-traces/version-7-count-1000-eflambe-output.bggg)
