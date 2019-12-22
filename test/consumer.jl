
@testset "consumer" begin
    kfksvr = get(ENV, "KAFKA_SERVER", nothing)
    tpar = get(ENV, "TOPIC_PARTITION", nothing)
    if isnothing(kfksvr) || isnothing(tpar)
        @warn "Skip test consumer, KAFKA_SERVER=$kfksvr, TOPIC_PARTITION=$tpar"
        return
    end

    topic, par = split(tpar, ':')
    topic = string(topic)
    par = parse(Int, par)
    @info "connect $kfksvr ..."
    c = KafkaConsumer(kfksvr, "rdkafka-test")
    parlist = [(topic, par)]
    @info "subscribe $topic:$par ..."
    #subscribe(c, parlist)
    assign(c, parlist)
    seek(c, (topic, par), 0, 10000)
    pollthis() = poll(String, String, c)
    pollfirst(n) = begin
        for i = 1:10
            msg = pollthis()
            isnothing(msg) || return msg
        end
    end

    n = 10
    msg = pollfirst(n)
    if isnothing(msg)
        @warn "No result with fetch $n times"
        return
    end
    seek(c, (topic, par), 0)
    @test pollfirst(n) == msg
end
