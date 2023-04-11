package io.streamnative.pulsar.handlers.mqtt.sharding;

public interface Sharder {
    void addShardId(String id);
    void removeShardId(String id);
    String getShardId(String topic);
}
