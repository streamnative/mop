package io.streamnative.pulsar.handlers.mqtt.sharding;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashSharder implements Sharder {
    private final SortedMap<Integer, String> circle = new TreeMap<>();
    private final int numberOfReplicas;
    private final HashFunction hashFunction;

    public ConsistentHashSharder(int numberOfReplicas, List<String> shardIds) {
        this.numberOfReplicas = numberOfReplicas;
        this.hashFunction = Hashing.sha256();
        for (String server : shardIds) {
            addShardId(server);
        }
    }

    @Override
    public void addShardId(String id) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hash(id + i), id);
        }
    }

    @Override
    public void removeShardId(String id) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hash(id + i));
        }
    }

    @Override
    public String getShardId(String topic) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hash(topic);
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    private int hash(String key) {
        return hashFunction.hashString(key, StandardCharsets.UTF_8).asInt();
    }
}

