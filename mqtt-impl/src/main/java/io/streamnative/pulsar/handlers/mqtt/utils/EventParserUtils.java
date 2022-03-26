package io.streamnative.pulsar.handlers.mqtt.utils;

import org.apache.pulsar.common.naming.TopicName;

public class EventParserUtils {
    public static TopicName parseFromManagedLedgerEvent(String path) {
        // managed-ledgers/public/default/persistent/topicName
        String[] pathArr = path.split("/");
        String tenant = pathArr[2];
        String namespace = pathArr[3];
        String topicDomain = pathArr[4];
        String topicName = pathArr[5];
        return TopicName.get(topicDomain, tenant, namespace, topicName);
    }
}
