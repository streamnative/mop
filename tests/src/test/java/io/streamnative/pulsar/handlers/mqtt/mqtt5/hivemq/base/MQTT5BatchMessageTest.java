package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.apache.pulsar.client.api.Producer;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The tests for MQTT protocol with batched messages.
 * This feature is the not protocol version limit. So, it doesn't need to use an MQTT3 re-test.
 */
public class MQTT5BatchMessageTest extends MQTTTestBase {

    @Test(timeOut = TIMEOUT)
    public void testReceiveBatchMessageWithCorrectPacketId() throws Exception {
        final String topic = "persistent://public/default/test-batch-message-1";
        final Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL, true);
        Producer<byte[]> producer = pulsarClient.newProducer()
                .enableBatching(true)
                .batchingMaxMessages(5)
                .topic(topic)
                .create();
        final List<String> payloads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final String payload = UUID.randomUUID().toString();
            payloads.add(payload);
            producer.sendAsync(payload.getBytes());
        }
        // The Hive MQ client doesn't have an API to get message packet id.
        // However, when the Hive MQ client receives the duplicate packet id, it will throw an exception.
        for (int i = 0; i < 5; i++) {
            Mqtt5Publish message = publishes.receive();
            message.acknowledge();
            String payload = new String(message.getPayloadAsBytes());
            Assert.assertTrue(payloads.contains(payload));
        }
        publishes.close();
        client.disconnect();
        producer.close();
    }
}
