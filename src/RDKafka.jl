module RDKafka

export KafkaProducer,
    KafkaConsumer,
    KafkaClient,
    # produce,
    subscribe,
    poll,
    assign

include("core.jl")

end # module
