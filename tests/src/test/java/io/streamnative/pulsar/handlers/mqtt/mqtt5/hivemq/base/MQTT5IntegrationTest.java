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

import com.google.common.collect.Lists;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectRestrictions;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class MQTT5IntegrationTest extends MQTTTestBase {

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testBasicPublishAndConsumeWithMQTT(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5Publish publish = publishes.receive();
            Assert.assertEquals(publish.getTopic(), MqttTopic.of(topic));
            Assert.assertEquals(publish.getPayloadAsBytes(), msg);
        }
        client.unsubscribeWith().topicFilter(topic).send();
        client.disconnect();
    }

    @Test(dataProvider = "mqttTopicNameAndFilter", timeOut = TIMEOUT)
    public void testTopicNameFilter(String topic, String filter) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        client.subscribeWith().topicFilter(filter).qos(MqttQos.AT_LEAST_ONCE).send();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5Publish publish = publishes.receive();
            Assert.assertEquals(publish.getPayloadAsBytes(), msg);
        }
        client.unsubscribeWith().topicFilter(filter).send();
        client.disconnect();
    }

    @Test(invocationCount = 2)
    public void testDynamicUpdateSubscribe() throws InterruptedException, PulsarAdminException {
        final String topicFilter = "/a/#";
        final String topic1 = "/a/b/c";
        final String topic2 = "/a/v/c";
        final String topic3 = "/a/z/c";
        List<String> messages = Collections.unmodifiableList(Lists.newArrayList("msg1", "msg2"));
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        client.subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        Mqtt5BlockingClient client2 = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client2.connect();
        client2.publishWith()
                .topic(topic1)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(messages.get(0).getBytes(StandardCharsets.UTF_8))
                .send();
        client2.publishWith()
                .topic(topic2)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(messages.get(1).getBytes(StandardCharsets.UTF_8))
                .send();
        Mqtt5Publish msg1 = publishes.receive();
        Assert.assertTrue(messages.contains(new String(msg1.getPayloadAsBytes())));
        Mqtt5Publish msg2 = publishes.receive();
        Assert.assertTrue(messages.contains(new String(msg2.getPayloadAsBytes())));
        client.unsubscribeWith()
                .topicFilter(topicFilter)
                .send();
        client2.publishWith()
                .topic(topic3)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(messages.get(1).getBytes(StandardCharsets.UTF_8))
                .send();
        Assert.assertFalse(publishes.receive(2, TimeUnit.SECONDS).isPresent());
        List<String> topics = Lists.newArrayList(Codec.encode(topic1), Codec.encode(topic2));
        for (String topic : topics) {
            TopicStats stats = admin.topics().getStats(topic);
            Assert.assertEquals(stats.getSubscriptions().size(), 1);
            SubscriptionStats subscriptionStats =
                    stats.getSubscriptions().get(client.getConfig().getClientIdentifier().get().toString());
            Assert.assertNotNull(subscriptionStats);
        }
        client.disconnect();
        // after disconnect all consumer will delete
        Awaitility.await().untilAsserted(()-> {
            for (String topic : topics) {
                TopicStats stats = admin.topics().getStats(topic);
                Assert.assertEquals(stats.getSubscriptions().size(), 0);
            }
        });
        client2.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testMaximumPacketSize() throws Exception {
        final String topic = "maximumPacketSize";
        final String identifier = "maximum-packet-size";
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(identifier)
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client.connectWith()
                .restrictions(
                Mqtt5ConnectRestrictions.builder().maximumPacketSize(20).build())
                .send();
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        byte[] msg = "payload_123456789_123456789".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();

        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> received = publishes.receive(3, TimeUnit.SECONDS);
            Assert.assertFalse(received.isPresent());
        }
        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().get(identifier).getUnackedMessages(), 0);
        client.unsubscribeWith().topicFilter(topic).send();
        client.disconnect();
    }

    @Test
    public void testLastWillMessage() throws Exception {
        final String topic = "testLastWillMessage";
        final String identifier = "test-Last-Will-Message";
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(identifier)
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        Mqtt5UserProperties userProperty = Mqtt5UserProperties.builder()
                .add("user-1", "value-1")
                .add("user-2", "value-2")
                .build();
        Mqtt5UserProperty userProperty1 = Mqtt5UserProperty.of("user-1", "value-1");
        Mqtt5UserProperty userProperty2 = Mqtt5UserProperty.of("user-2", "value-2");
        client.connectWith().willPublish(Mqtt5Publish.builder().topic(topic).userProperties(userProperty)
                            .asWill().payload("online".getBytes(StandardCharsets.UTF_8)).build())
                .send();
        Mqtt5BlockingClient client2 = Mqtt5Client.builder()
                .identifier(identifier + "-client-2")
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client2.connectWith().send();
        client2.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL);
        client.disconnect();
        Mqtt5Publish message = publishes.receive();
        Assert.assertNotNull(message);
        Assert.assertEquals(message.getPayloadAsBytes(), "online".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(message.getUserProperties().asList().size(), 2);
        // Validate the user properties order, must be the same with set order.
        Assert.assertEquals(message.getUserProperties().asList().get(0).compareTo(userProperty1), 0);
        Assert.assertEquals(message.getUserProperties().asList().get(1).compareTo(userProperty2), 0);
        publishes.close();
        client2.unsubscribeWith().topicFilter(topic).send();
        client2.disconnect();
    }

    @Test
    public void testTopicAlias() throws InterruptedException {
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        Mqtt5ConnAck connAck = client.connectWith()
                .restrictions()
                .topicAliasMaximum(10)
                .sendTopicAliasMaximum(10)
                .applyRestrictions()
                .send();
        Assert.assertEquals(connAck.getRestrictions().getTopicAliasMaximum(), 10);
        final String topicName = "a/b/c";
        client.subscribeWith()
                .topicFilter(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        List<String> contents = new ArrayList<>();
        client.toAsync().publishes(MqttGlobalPublishFilter.ALL, (msg) -> {
            contents.add(new String(msg.getPayloadAsBytes()));
        });
        client.publishWith()
                .topic(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("hihihi1".getBytes(StandardCharsets.UTF_8))
                .send();
        client.publishWith()
                .topic(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("hihihi2".getBytes(StandardCharsets.UTF_8))
                .send();
        client.publishWith()
                .topic(topicName)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("hihihi3".getBytes(StandardCharsets.UTF_8))
                .send();
        Awaitility.await()
                .untilAsserted(() -> {
                    Assert.assertEquals(contents.size(), 3);
                    Assert.assertTrue(contents.contains("hihihi1"));
                    Assert.assertTrue(contents.contains("hihihi2"));
                    Assert.assertTrue(contents.contains("hihihi3"));
                });
        client.disconnect();
    }

    @Test
    public void testResubscribe() throws Exception {
        final String topic = "testResubscribe";
        final String identifier = "testResubscribe";
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(identifier)
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client.connectWith().send();
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        client.unsubscribeWith().topicFilter(topic).send();
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        client.disconnect();
    }
}
