## consumer

mutable struct KafkaConsumer
    client::KafkaClient
    rkparlist::Ptr{Cvoid}
end


function KafkaConsumer(conf::Dict)
    @assert haskey(conf, "bootstrap.servers") "`bootstrap.servers` should be specified in conf"
    @assert haskey(conf, "group.id") "`group.id` should be specified in conf"
    client = KafkaClient(KAFKA_TYPE_CONSUMER, conf)
    rkparlist = kafka_topic_partition_list_new()
    consumer = KafkaConsumer(client, rkparlist)
    finalizer(c -> kafka_topic_partition_list_destroy(c.rkparlist), consumer)
    return consumer
end


function KafkaConsumer(bootstrap_servers::String, group_id::String, conf::Dict=Dict())
    conf["bootstrap.servers"] = bootstrap_servers
    conf["group.id"] = group_id
    return KafkaConsumer(conf)
end


function Base.show(io::IO, c::KafkaConsumer)
    group_id = c.client.conf["group.id"]
    bootstrap_servers = c.client.conf["bootstrap.servers"]
    print(io, "KafkaConsumer($group_id @ $bootstrap_servers)")
end


function subscribe(c::KafkaConsumer, topic_partitions::Vector{Tuple{String, Int}})
    rkparlist = c.rkparlist
    for (topic, partition) in topic_partitions
        kafka_topic_partition_list_add(rkparlist, topic, partition)
    end
    kafka_subscribe(c.client.rk, rkparlist)
end


function poll(::Type{K}, ::Type{P}, c::KafkaConsumer, timeout::Int=1000) where {K,P}
    c_msg_ptr = kafka_consumer_poll(c.client.rk, timeout)
    if c_msg_ptr != nothing
        c_msg = unsafe_load(c_msg_ptr)
        msg = Message{K,P}(c_msg)
        kafka_message_destroy(c_msg_ptr)
        return msg
    else
        return nothing
    end
end


poll(c::KafkaConsumer, timeout::Int=1000) = poll(Vector{UInt8}, Vector{UInt8}, c, timeout)

function seek(c::KafkaConsumer,
              topic_partition::Tuple{String, Integer},
              offset::Integer,
              timeout::Integer=1000)
    topic, par = topic_partition
    if !kafka_topic_partition_list_find(c.rkparlist, topic, par)
        error("Seek on an assigned/subscribed topic partition $topic_partition")
    end
    kt = KafkaTopic(c.client, topic)
    kafka_seek(kt.rkt, par, offset, timeout)
end
