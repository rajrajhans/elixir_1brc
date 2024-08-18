random_num = :rand.uniform(1_000_000)
Benchee.run(
    %{
        "v7" => fn -> OneBRC.MeasurementsProcessor.process(10_000_000, "7") end,
        "v6" => fn -> OneBRC.MeasurementsProcessor.process(10_000_000, "6") end
    },
    warmup: 5,
    formatters: [
        {Benchee.Formatters.HTML, file: "data/benchee_#{random_num}.html"},
        Benchee.Formatters.Console
    ]
)
