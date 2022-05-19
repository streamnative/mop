/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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


    @Test
    public void testAckBatchMessageIndividual() throws Exception {
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
        for (int i = 0; i < 50; i++) {
            final String payload = UUID.randomUUID().toString();
            payloads.add(payload);
            producer.sendAsync(payload.getBytes());
        }
        for (int i = 0; i < 50; i++) {
            Mqtt5Publish message = publishes.receive();
            if (i % 2 == 0)  {
                message.acknowledge();
                String payload = new String(message.getPayloadAsBytes());
                payloads.remove(payload);
            }

        }
        admin.topics().unload(topic);

        Assert.assertFalse(payloads.isEmpty());
        for (int i = 0; i < 50; i++) {
            Optional<Mqtt5Publish> receive = publishes.receive(5, TimeUnit.SECONDS);
            if (receive.isPresent()) {
                Mqtt5Publish message = receive.get();
                message.acknowledge();
                String payload = new String(message.getPayloadAsBytes());
                payloads.remove(payload);
            }
        }
        Assert.assertTrue(payloads.isEmpty());
        publishes.close();
        client.disconnect();
        producer.close();
    }
}
