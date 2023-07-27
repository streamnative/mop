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
package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.proxy;

import com.google.common.collect.Lists;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base.MQTT5ClientUtils;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MQTT5ProxyIntegrationTest extends MQTTTestBase {
    private final Random random = new Random();

    @Override
    public MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        return conf;
    }

    @Test(timeOut = 30_000)
    public void testBrokerThrowServiceNotReadyException() throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        client.connect();
        final String topic1 = "topic-1";
        final String ownedBroker = admin.lookups().lookupTopic(topic1);

        final Mqtt5PublishResult r1 = client.publishWith()
                .topic(topic1)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("msg1".getBytes(StandardCharsets.UTF_8))
                .send();
        Assert.assertFalse(r1.getError().isPresent());
        while (ownedBroker.equals(admin.lookups().lookupTopic(topic1))) {
            admin.namespaces().unload("public/default");
            Thread.sleep(1000);
        }
        try {
            final Mqtt5PublishResult r2 = client.publishWith()
                    .topic(topic1)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload("msg1".getBytes(StandardCharsets.UTF_8))
                    .send();
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof Mqtt5PubAckException);
            Assert.assertEquals(((Mqtt5PubAckException) ex).getMqttMessage().getReasonCode().getCode()
                    , Mqtt5PubReasonCode.UNSPECIFIED_ERROR.value());
        }
        final Mqtt5PublishResult r3 = client.publishWith()
                .topic(topic1)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("msg1".getBytes(StandardCharsets.UTF_8))
                .send();
        Assert.assertFalse(r3.getError().isPresent());
        client.disconnect();
    }

    @Test(invocationCount = 2)
    public void testDynamicUpdateSubscribe() throws InterruptedException, PulsarAdminException {
        final String topicFilter = "/a/#";
        final String topic1 = "/a/b/c";
        final String topic2 = "/a/v/c";
        final String topic3 = "/a/z/c";
        List<String> messages = Collections.unmodifiableList(Lists.newArrayList("msg1", "msg2"));
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        client.connect();
        client.subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        Mqtt5BlockingClient client2 = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
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

}
