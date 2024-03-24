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

#### Pre-calculating the whole file in memory, and then writing it in single shot

#### Adding concurrency to the pre-calculation

- weird behaviour with one billion rows
