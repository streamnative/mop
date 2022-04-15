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
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
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

    @Test
    public void testDynamicUpdateSubscribe() throws InterruptedException, PulsarAdminException {
        final String topicFilter = "/a/#";
        final String topic1 = "/a/b/c";
        final String topic2 = "/a/v/c";
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

}
