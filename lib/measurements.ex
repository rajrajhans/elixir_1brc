# adapted from Java version in 1brc repo (https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CreateMeasurements.java)

defmodule OneBRC.Measurements do
  @measurements_file "./data/measurements.txt"
  @count 1_000_000_000

  alias OneBRC.Measurements.WeatherStations
  alias OneBRC.Measurements.WeatherStation
  alias OneBRC.Measurements.Data
  require Logger

  def create_measurements() do
    create_measurements(@count)
  end

  def create_measurements(count) do
    _ = File.rm(@measurements_file)
    :ok = File.touch(@measurements_file)

    Logger.info("Creating #{count} measurements ...")

    {time, _} = :timer.tc(fn -> create_measurements_(count) end)
    time_s = round(time / 1_000_000 * 10) / 10.0
    Logger.info("Created #{count} measurements in #{time_s}s")
  end

  def create_measurements_(count) do
    stations = Data.stations_data() |> Map.keys()
    num_stations = Enum.count(stations)

    t1 = System.monotonic_time(:millisecond)

    content =
      1..count
      |> Enum.map(fn _ ->
        station = Enum.at(stations, :rand.uniform(num_stations - 1))
        ws = WeatherStation.measurement(station)

        "#{ws.name};#{ws.temperature}\n"
      end)
      |> Enum.join()

    t2 = System.monotonic_time(:millisecond)

    {:ok, file} = File.open(@measurements_file, [:append, :utf8])
    IO.write(file, content)

    t3 = System.monotonic_time(:millisecond)

    File.close(file)

    Logger.info("time to create measurements: #{t2 - t1}ms")
    Logger.info("time to write measurements: #{t3 - t2}ms")
  end
end

defmodule OneBRC.Measurements.WeatherStation do
  alias OneBRC.Measurements.WeatherStations
  alias __MODULE__

  defstruct [:name, :temperature]

  def measurement(name) do
    station = WeatherStations.get_station(name)
    temp = with_rand_fluctuation(station.temperature)
    temp = round(temp * 10.0) / 10.0

    %WeatherStation{name: station.name, temperature: temp}
  end

  defp with_rand_fluctuation(temp) do
    rounded_temp = round(temp)
    # random number between -1 and 1
    rand_1 = :rand.uniform() * -2 + 1

    # random number between 0 and abs(rounded_temp)
    rand_2 =
      cond do
        rounded_temp == 0 -> 0
        true -> :rand.uniform(abs(rounded_temp))
      end

    temp + rand_1 * rand_2
  end
end

defmodule OneBRC.Measurements.WeatherStations do
  alias OneBRC.Measurements.WeatherStation
  alias OneBRC.Measurements.Data

  def get_station(name) do
    stations = Data.stations_data()
    temp = Map.fetch!(stations, name)
    %WeatherStation{name: name, temperature: temp}
  end
end

defmodule OneBRC.Measurements.Data do
  @stations_data %{
    "N'Djamena": 28.3,
    Toluca: 12.4,
    Luanda: 25.8,
    Tabora: 23.0,
    Bouaké: 26.0,
    "Las Vegas": 20.3,
    "Pointe-Noire": 26.1,
    Singapore: 27.0,
    Conakry: 26.4,
    Ankara: 12.0,
    "New Orleans": 20.7,
    Gangtok: 15.2,
    Porto: 15.7,
    Kabul: 12.1,
    Makurdi: 26.0,
    Lagos: 26.8,
    Marseille: 15.8,
    Oranjestad: 28.1,
    Murmansk: 0.6,
    Tunis: 18.4,
    Madrid: 15.0,
    Reykjavík: 4.3,
    Thessaloniki: 16.0,
    Canberra: 13.1,
    Albuquerque: 14.0,
    Changsha: 17.4,
    Istanbul: 13.9,
    Valencia: 18.3,
    Iqaluit: -9.3,
    Malabo: 26.3,
    Dallas: 19.0,
    Parakou: 26.8,
    Muscat: 28.0,
    Wau: 27.8,
    Kandi: 27.7,
    Halifax: 7.5,
    "Chiang Mai": 25.8,
    Jayapura: 27.0,
    Tbilisi: 12.9,
    Astana: 3.5,
    Houston: 20.8,
    Dikson: -11.1,
    Minneapolis: 7.8,
    Antananarivo: 17.9,
    Sarajevo: 10.1,
    Benghazi: 19.9,
    Seattle: 11.3,
    Saskatoon: 3.3,
    Pittsburgh: 10.8,
    Anadyr: -6.9,
    Sacramento: 16.3,
    "Tel Aviv": 20.0,
    Jacksonville: 20.3,
    Anchorage: 2.8,
    Warsaw: 8.5,
    Manama: 26.5,
    Berlin: 10.3,
    Beirut: 20.9,
    "Cabo San Lucas": 23.9,
    "Luxembourg City": 9.3,
    Ljubljana: 10.9,
    "Hat Yai": 27.0,
    Seville: 19.2,
    "Da Nang": 25.8,
    "Port Vila": 24.3,
    Busan: 15.0,
    Lyon: 12.5,
    Calgary: 4.4,
    Odienné: 26.0,
    Ngaoundéré: 22.0,
    Tallinn: 6.4,
    Toamasina: 23.4,
    Heraklion: 18.9,
    Kingston: 27.4,
    Tijuana: 17.8,
    Dodoma: 22.7,
    Dublin: 9.8,
    Taipei: 23.0,
    Bilbao: 14.7,
    "St. Louis": 13.9,
    Dakar: 24.0,
    Ashgabat: 17.1,
    Bishkek: 11.3,
    Jerusalem: 18.3,
    Ürümqi: 7.4,
    Colombo: 27.4,
    Sapporo: 8.9,
    "Virginia Beach": 15.8,
    Milwaukee: 8.9,
    "Lake Tekapo": 8.7,
    Mzuzu: 17.7,
    Juba: 27.8,
    Ifrane: 11.4,
    "La Paz": 23.7,
    Milan: 13.0,
    Harare: 18.4,
    "Port Sudan": 28.4,
    Baltimore: 13.1,
    Dhaka: 25.9,
    Vilnius: 6.0,
    Paris: 12.3,
    Bordeaux: 14.2,
    "Mek'ele": 22.7,
    "Phnom Penh": 28.3,
    Djibouti: 29.9,
    Mexicali: 23.1,
    Louisville: 13.9,
    "Guatemala City": 20.4,
    Baguio: 19.5,
    Kankan: 26.5,
    Batumi: 14.0,
    Toliara: 24.1,
    Chicago: 9.8,
    Yangon: 27.5,
    "El Paso": 18.1,
    Pyongyang: 10.8,
    Mumbai: 27.1,
    Darwin: 27.6,
    Baghdad: 22.77,
    Gaborone: 21.0,
    Brazzaville: 25.0,
    Nicosia: 19.7,
    Libreville: 25.9,
    Riga: 6.2,
    Kinshasa: 25.3,
    Tucson: 20.9,
    Kolkata: 26.7,
    Johannesburg: 15.5,
    "Santo Domingo": 25.9,
    Tokyo: 15.4,
    Indianapolis: 11.8,
    Detroit: 10.0,
    Lomé: 26.9,
    Reggane: 28.3,
    Managua: 27.3,
    Chongqing: 18.6,
    "Oklahoma City": 15.9,
    Helsinki: 5.9,
    Praia: 24.4,
    Marrakesh: 19.6,
    Lubumbashi: 20.8,
    Kunming: 15.7,
    Mandalay: 28.0,
    Dubai: 26.9,
    Banjul: 26.0,
    Rome: 15.2,
    Nouakchott: 25.7,
    Chittagong: 25.9,
    "San Salvador": 23.1,
    Sydney: 17.7,
    "Dar es Salaam": 25.8,
    "Palmerston North": 13.2,
    Skopje: 12.4,
    Kampala: 20.0,
    Lodwar: 29.3,
    Yakutsk: -8.8,
    Kyoto: 15.8,
    Arkhangelsk: 1.3,
    Riyadh: 26.0,
    Ouagadougou: 28.3,
    Oslo: 5.7,
    Bosaso: 30.0,
    Amsterdam: 10.2,
    "Washington, D.C.": 14.6,
    Melbourne: 15.1,
    Aden: 29.1,
    Vienna: 10.4,
    Hobart: 12.7,
    Gagnoa: 26.0,
    Damascus: 17.0,
    "Sana'a": 20.0,
    Hargeisa: 21.7,
    "Salt Lake City": 11.6,
    Upington: 20.4,
    "Cape Town": 16.2,
    Vladivostok: 4.9,
    Dampier: 26.4,
    Launceston: 13.1,
    Almaty: 10.0,
    Suwałki: 7.2,
    Fianarantsoa: 17.9,
    Abha: 18.0,
    Bergen: 7.7,
    Tauranga: 14.8,
    Edmonton: 4.2,
    Honiara: 26.5,
    Yaoundé: 23.8,
    "Hanga Roa": 20.5,
    Hamburg: 9.7,
    Wichita: 13.9,
    Austin: 20.7,
    Timbuktu: 28.0,
    Phoenix: 23.9,
    "Kansas City": 12.5,
    "San Antonio": 20.8,
    Tashkent: 14.8,
    Auckland: 15.2,
    London: 11.3,
    Manila: 28.4,
    Dolisie: 24.0,
    Ségou: 28.0,
    Guadalajara: 20.9,
    Irkutsk: 1.0,
    Ouahigouya: 28.6,
    Bloemfontein: 15.6,
    Zagreb: 10.7,
    Wrocław: 9.6,
    Tabriz: 12.6,
    Maputo: 22.8,
    Perth: 18.7,
    "Belize City": 26.7,
    "San Juan": 27.2,
    Sofia: 10.6,
    "Portland {OR}": 12.4,
    "Rostov-on-Don": 9.9,
    Oulu: 2.7,
    "Los Angeles": 18.6,
    Lhasa: 7.6,
    Erzurum: 5.1,
    "George Town": 27.9,
    Cairo: 21.4,
    Birao: 26.5,
    Split: 16.1,
    Moncton: 6.1,
    Ghanzi: 21.4,
    Accra: 26.4,
    Malé: 28.0,
    Nouadhibou: 21.3,
    Durban: 20.6,
    Vientiane: 25.9,
    Fresno: 17.9,
    Boise: 11.4,
    "City of San Marino": 11.8,
    Asmara: 15.6,
    Tegucigalpa: 21.7,
    Kyiv: 8.4,
    Alexandra: 11.0,
    Atlanta: 17.0,
    Brussels: 10.5,
    Bangkok: 28.6,
    Podgorica: 15.3,
    Napier: 14.6,
    Adelaide: 17.3,
    Athens: 19.2,
    "Port Moresby": 26.9,
    Brisbane: 21.4,
    Willemstad: 28.0,
    "Addis Ababa": 16.0,
    Zürich: 9.3,
    "Da Lat": 17.9,
    Frankfurt: 10.6,
    Bulawayo: 18.9,
    Mogadishu: 27.1,
    Rabat: 17.2,
    "Petropavlovsk-Kamchatsky": 1.9,
    Novosibirsk: 1.7,
    Chihuahua: 18.6,
    Erbil: 19.5,
    Napoli: 15.9,
    Livingstone: 21.8,
    Bujumbura: 23.8,
    Mango: 28.1,
    Dunedin: 11.1,
    Nairobi: 17.8,
    Kathmandu: 18.3,
    Nashville: 15.4,
    Columbus: 11.7,
    "La Ceiba": 26.2,
    Havana: 25.2,
    "San Diego": 17.8,
    "Kuwait City": 25.7,
    Lisbon: 17.5,
    Palembang: 27.3,
    Christchurch: 12.2,
    Tirana: 15.2,
    Montreal: 6.8,
    Antsiranana: 25.2,
    Abidjan: 26.0,
    Rangpur: 24.4,
    Charlotte: 16.1,
    Shanghai: 16.7,
    Bamako: 27.8,
    Edinburgh: 9.3,
    Fukuoka: 17.0,
    Memphis: 17.2,
    Guangzhou: 22.4,
    Lviv: 7.8,
    Yinchuan: 9.0,
    Niamey: 29.3,
    Dili: 26.6,
    Niigata: 13.9,
    "Xi'an": 14.1,
    "Nakhon Ratchasima": 27.3,
    Surabaya: 27.1,
    Tampa: 22.9,
    Cotonou: 27.2,
    "Las Palmas de Gran Canaria": 21.2,
    "Panama City": 28.0,
    Tromsø: 2.9,
    Entebbe: 21.0,
    Whitehorse: -0.1,
    İzmir: 17.9,
    Budapest: 11.3,
    Suva: 25.6,
    Nassau: 24.6,
    Hiroshima: 16.3,
    "Mexico City": 17.5,
    "Lake Havasu City": 23.7,
    Hamilton: 13.8,
    Honolulu: 25.4,
    "Ho Chi Minh City": 27.4,
    "New York City": 12.9,
    Valletta: 18.8,
    Garissa: 29.3,
    Kano: 26.4,
    Barcelona: 18.2,
    Minsk: 6.7,
    Burnie: 13.1,
    "Saint-Pierre": 5.7,
    Pontianak: 27.7,
    Ouarzazate: 18.9,
    Douala: 26.7,
    "Kuala Lumpur": 27.3,
    Hanoi: 23.6,
    Bridgetown: 27.0,
    Kumasi: 26.0,
    Abéché: 29.4,
    Bata: 25.1,
    Wellington: 12.9,
    "Flores,  Petén": 26.4,
    Odesa: 10.7,
    Baku: 15.1,
    Bratislava: 10.5,
    Jakarta: 26.7,
    Copenhagen: 9.1,
    Roseau: 26.2,
    "Port-Gentil": 26.0,
    Garoua: 28.3,
    Beijing: 12.9,
    "Hong Kong": 23.3,
    Monaco: 16.4,
    Sochi: 14.2,
    Winnipeg: 3.0,
    Seoul: 12.5,
    Tripoli: 20.0,
    Algiers: 18.2,
    Ndola: 20.3,
    Naha: 23.1,
    Harbin: 5.0,
    Tehran: 17.0,
    Fairbanks: -2.3,
    Veracruz: 25.4,
    Tamale: 27.9,
    Makassar: 26.7,
    Maun: 22.4,
    Khartoum: 29.9,
    Prague: 8.4,
    "Alice Springs": 21.0,
    Lahore: 24.3,
    Omaha: 10.6,
    Karachi: 26.0,
    Bucharest: 10.8,
    Kuopio: 3.4,
    Pretoria: 18.2,
    Sokoto: 28.0,
    Toronto: 9.4,
    Nuuk: -1.4,
    Ahvaz: 25.4,
    Alexandria: 20.0,
    Cairns: 25.0,
    Medan: 26.5,
    Karonga: 24.4,
    Villahermosa: 27.1,
    Blantyre: 22.2,
    Belgrade: 12.5,
    Gabès: 19.5,
    Stockholm: 6.6,
    Denpasar: 23.7,
    Bangui: 26.0,
    Thiès: 24.0,
    Monterrey: 22.3,
    Yerevan: 12.4,
    Chișinău: 10.2,
    Moscow: 5.8,
    "Palm Springs": 24.5,
    Vancouver: 10.4,
    Ulaanbaatar: -0.4,
    Vaduz: 10.1,
    Assab: 30.5,
    Lusaka: 19.9,
    Ottawa: 6.6,
    "New Delhi": 25.0,
    Yellowknife: -4.3,
    Boston: 10.9,
    "Saint Petersburg": 5.8,
    Philadelphia: 13.2,
    Cracow: 9.3,
    Dushanbe: 14.7,
    Tamanrasset: 21.7,
    "Andorra la Vella": 9.8,
    Bissau: 27.0,
    Miami: 24.9,
    "St. John's": 5.0,
    "San José": 22.6,
    "Gjoa Haven": -14.4,
    Mombasa: 26.3,
    Palermo: 18.5,
    "Zanzibar City": 26.0,
    "San Jose": 16.4,
    Denver: 10.4,
    "San Francisco": 14.6,
    Jos: 22.8,
    Mahajanga: 26.3
  }

  def stations_data, do: @stations_data
end
