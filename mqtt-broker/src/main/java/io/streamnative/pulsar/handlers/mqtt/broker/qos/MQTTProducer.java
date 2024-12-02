package io.streamnative.pulsar.handlers.mqtt.broker.qos;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;

public class MQTTProducer extends Producer {

    public static final AtomicLong PRODUCER_ID = new AtomicLong();

    public MQTTProducer(Topic topic, TransportCnx cnx, long producerId, String producerName, String appId,
                        boolean isEncrypted, Map<String, String> metadata, SchemaVersion schemaVersion, long epoch,
                        boolean userProvidedProducerName, ProducerAccessMode accessMode, Optional<Long> topicEpoch,
                        boolean supportsPartialProducer) {
        super(topic, cnx, producerId, producerName, appId, isEncrypted, metadata, schemaVersion, epoch,
                userProvidedProducerName, accessMode, topicEpoch, supportsPartialProducer);
    }

    public static MQTTProducer create(Topic topic, TransportCnx cnx, String producerName) {
        return new MQTTProducer(topic, cnx, PRODUCER_ID.incrementAndGet(), producerName, "",
                false, null, SchemaVersion.Latest, 0, true,
                ProducerAccessMode.Shared, Optional.empty(), true);
    }
}
