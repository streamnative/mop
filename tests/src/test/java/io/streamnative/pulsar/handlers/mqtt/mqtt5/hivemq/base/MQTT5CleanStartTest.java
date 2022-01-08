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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5CleanStartTest extends MQTTTestBase {

    @Test(timeOut = TIMEOUT)
    public void testCleanSession() throws Exception {
        final String topic = "clean-session-test-1";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connectWith()
                .cleanStart(true)
                .send();
        boolean sessionPresent = connect.isSessionPresent();
        Assert.assertFalse(sessionPresent);
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        String pulsarTopicName = PulsarTopicUtils
                .getPulsarTopicName(topic, defaultTenant, defaultNamespace, true, defaultTopicDomain);
        List<String> subscriptions = admin.topics().getSubscriptions(pulsarTopicName);
        String clientId = Objects.requireNonNull(client.getConfig().getClientIdentifier().orElse(null)).toString();
        Assert.assertTrue(subscriptions.contains(clientId));
        client.disconnect();
        Awaitility.await()
                .untilAsserted(() ->
                        Assert.assertTrue(CollectionUtils.isEmpty(admin.topics().getSubscriptions(pulsarTopicName))));
    }

    @Test(timeOut = TIMEOUT)
    public void testNotCleanSession() throws Exception {
        final String topic = "clean-session-test-2";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(0xffffffffL)
                .send();
        boolean sessionPresent = connect.isSessionPresent();
        Assert.assertTrue(sessionPresent);
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        String pulsarTopicName = PulsarTopicUtils
                .getPulsarTopicName(topic, defaultTenant, defaultNamespace, true, defaultTopicDomain);
        List<String> subscriptions = admin.topics().getSubscriptions(pulsarTopicName);
        String clientId = Objects.requireNonNull(client.getConfig().getClientIdentifier().orElse(null)).toString();
        Assert.assertTrue(subscriptions.contains(clientId));
        client.disconnect();
        Awaitility.await().pollDelay(1, TimeUnit.SECONDS)
                        .untilAsserted(()->
                                Assert.assertTrue(admin.topics().getSubscriptions(pulsarTopicName).contains(clientId)));
    }
}
