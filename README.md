## The One Billion Row Challenge (in Elixir)

1BRC is a challenge, [originally in Java](https://github.com/gunnarmorling/1brc) to process a text file of weather station names and temperatures, and for each weather station, print out the minimum, mean, and maximum. It sounds simple, but the catch is that the file contains one billion rows. Fun!

I took to solve the challenge in two parts - first being to optimize the creation of one billion measurements, and then to actually process them as per the original challenge.

## Part 1: Writing 1 billion measurements to a file

- The goal here is to create a file with one billion rows, each row containing a weather station name and a temperature.
- There are baseline temperatures for each station given, and the temperature for each row is the baseline temperature for that station plus a random number between -10 and 10.
- The file is a text file with each line of the format `station_name;temperature`.

Approaches:
