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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base.MQTT5ClientUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class MQTT5SessionExpireIntervalTest extends MQTTTestBase {
    private final Random random = new Random();

    @Override
    public MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        return conf;
    }


    @Test(timeOut = TIMEOUT)
    public void testIntervalCleanSession() throws Exception {
        final String topic = "test-expire-interval-1";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        Mqtt5ConnAck connect = client.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(5)
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
        List<String> afterDisconnectionSubscriptions = admin.topics().getSubscriptions(pulsarTopicName);
        Assert.assertTrue(afterDisconnectionSubscriptions.contains(clientId));
        Awaitility
                .await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<String> delaySubscriptions = admin.topics().getSubscriptions(pulsarTopicName);
                    Assert.assertTrue(CollectionUtils.isEmpty(delaySubscriptions));
                });
    }

    @Test(timeOut = TIMEOUT)
    public void testStopExpireIntervalWhenClientReconnect() throws Exception {
        final String topic = "test-expire-interval-2";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        Mqtt5ConnAck connect = client.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(5)
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
        List<String> afterDisconnectionSubscriptions = admin.topics().getSubscriptions(pulsarTopicName);
        Assert.assertTrue(afterDisconnectionSubscriptions.contains(clientId));
        Mqtt5ConnAck reconnect = client.connectWith()
                .cleanStart(false)
                .sessionExpiryInterval(5)
                .send();
        boolean reconnectSessionPresent = reconnect.isSessionPresent();
        Assert.assertTrue(reconnectSessionPresent);
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Awaitility.await()
                .pollDelay(8, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<String> reconnectSubscriptions = admin.topics().getSubscriptions(pulsarTopicName);
                    Assert.assertTrue(reconnectSubscriptions.contains(clientId));
                });
    }


}
