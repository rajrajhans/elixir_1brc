## The One Billion Row Challenge (in Elixir)

1BRC is a challenge, [originally in Java](https://github.com/gunnarmorling/1brc) to process a text file of weather station names and temperatures, and for each weather station, print out the minimum, mean, and maximum. It sounds simple, but the catch is that the file contains one billion rows. Fun!

I took to solve the challenge in two parts - first being to optimize the creation of one billion measurements, and then to actually process them as per the original challenge.

## Part 1: Writing measurements to a file

- The goal here is to create a file with one billion rows, each row containing a weather station name and a temperature.
- There are baseline temperatures for each station given, and the temperature for each row is the baseline temperature for that station plus a random number between -10 and 10.
- The file is a text file with each line of the format `station_name;temperature`.
- The fastest solution I've implemented so far creates the file with 1 billion measurements in 205 seconds. The code is at [`lib/measurements_generator.ex`](lib/measurements_generator.ex).
- For more details, check out the [doc](docs/part_1_writing_billion_measurements.md).

### Commands for creating measurements:

- `run create_measurements 1000` - creates a file `./data/measurements.1000.txt` with 1000 measurements. You can change 1000 to the number of measurements you want.
- `run create_measurements.profile 1000` - creates a file `./data/measurements.1000.txt` with 1000 measurements and profiles the execution using `eprof`.
- Check `./bin/run` to see what the above commands do.

## Part 2: Processing measurements

- WIP.
- For more details, check out the [doc](docs/part_2_processing_billion_measurements.md).

### Commands for processing measurements:

- `run create_baseline_results 1000` - creates a file `./data/result_baseline.10000.json` with the correct results for 1000 measurements. This file can then be used to verify the correctness of results we get while trying to optimize the processing.
- `run process_measurements 1000` - processes the file `./data/measurements.1000.txt` with 1000 measurements, generates `result.10000.json` & verifies their correctness with the file `./data/result_baseline.10000.json`.
- `run process_measurements.profile 1000` - processes the file `./data/measurements.1000.txt` with 1000 measurements and profiles the execution using `eprof`.
- `run all_versions --count 10000` - runs all versions of the measurements processor with 10000 measurements. This will create a file `./tmp/all_versions_outputs/version-v_count-10000.txt` with the results of all versions. Useful to measure time taken by each version.
- Check `./bin/run` to see what the above commands do.

## Setting up this repo

- This repo uses Nix flakes to manage dependencies. To get started, run `direnv allow` in the root of the repo to activate the Nix environment.
- Execute `run deps` to install dependencies.
- To create measurements file, execute `run create_measurements 1_000_000`. This will create a file `data/measurements.1000000.txt` with 1 million measurements.
- To process the measurements, execute `run process_measurements 1_000_000`. This will process the file created in the previous step.
- That's it, you can follow the codepaths, starting from `bin/run` to explore further ✌️
