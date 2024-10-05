random_num = :rand.uniform(1_000_000)
Benchee.run(
    %{
        "v3" => fn -> OneBRC.MeasurementsProcessor.process(1000, "3") end,
        "v2" => fn -> OneBRC.MeasurementsProcessor.process(1000, "2") end
    },
    warmup: 2,
    formatters: [
        {Benchee.Formatters.HTML, file: "data/benchee_#{random_num}.html"},
        Benchee.Formatters.Console
    ]
)
