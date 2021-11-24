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
package io.streamnative.pulsar.handlers.mqtt.mqtt5;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.AuthorizationConfig;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MQTT5AuthorizationProxyReasonCodeOnAllAckTest extends AuthorizationConfig {
    private final Random random = new Random();

    @Override
    public MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        return conf;
    }

    @Test(timeOut = TIMEOUT)
    public void testSubScribeAuthorized() throws PulsarAdminException, InterruptedException {
        Set<AuthAction> user1Actions = new HashSet<>();
        user1Actions.add(AuthAction.produce);
        admin.namespaces().grantPermissionOnNamespace("public/default", "user1", user1Actions);

        Set<AuthAction> user2Actions = new HashSet<>();
        user2Actions.add(AuthAction.consume);
        admin.namespaces().grantPermissionOnNamespace("public/default", "user2", user2Actions);

        String topicName = "persistent://public/default/testAuthorization";

        String message = "Hello MQTT Pulsar";

        Mqtt5BlockingClient publisher = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        publisher.connectWith()
                .simpleAuth()
                .username("user1")
                .password("pass1".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .send();

        Mqtt5BlockingClient consumer = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        consumer.connectWith()
                .simpleAuth()
                .username("user2")
                .password("pass2".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .send();
        Mqtt5SubAck consumerAck = consumer.subscribeWith().topicFilter(topicName).qos(MqttQos.AT_LEAST_ONCE).send();
        publisher.publishWith()
                .topic(topicName)
                .payload(message.getBytes(StandardCharsets.UTF_8))
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        for (Mqtt5SubAckReasonCode reasonCode : consumerAck.getReasonCodes()) {
            Assert.assertEquals(reasonCode, Mqtt5SubAckReasonCode.GRANTED_QOS_1);
        }
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = consumer.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5Publish publish = publishes.receive();
            Assert.assertEquals(publish.getPayloadAsBytes(), message.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test(timeOut = TIMEOUT)
    public void testSubScribeNotAuthorized() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(getMqttBrokerPortList().get(0));
        client.connectWith()
                .simpleAuth()
                .username("user1")
                .password("pass1".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .send();
        try {
            client.subscribeWith().topicFilter("a").qos(MqttQos.AT_LEAST_ONCE).send();
        } catch (Mqtt5SubAckException ex) {
            for (Mqtt5SubAckReasonCode reasonCode : ex.getMqttMessage().getReasonCodes()) {
                Assert.assertEquals(reasonCode, Mqtt5SubAckReasonCode.NOT_AUTHORIZED);
            }
        }
        Awaitility.await()
                .untilAsserted(() -> Assert.assertFalse(client.getState().isConnected()));
    }

    @Test(timeOut = TIMEOUT)
    public void testPublishNotAuthorized() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(getMqttBrokerPortList().get(0));
        client.connectWith()
                .simpleAuth()
                .username("user1")
                .password("pass1".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .send();
        String message = "Hello MQTT Pulsar";
        try {
            client.publishWith()
                    .topic("a")
                    .payload(message.getBytes(StandardCharsets.UTF_8))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send();
            client.subscribeWith().topicFilter("a").qos(MqttQos.AT_LEAST_ONCE).send();
        } catch (Mqtt5PubAckException ex) {
            Mqtt5PubAckReasonCode reasonCode = ex.getMqttMessage().getReasonCode();
            Assert.assertEquals(reasonCode, Mqtt5PubAckReasonCode.NOT_AUTHORIZED);
        }
        Awaitility.await()
                .untilAsserted(() -> Assert.assertFalse(client.getState().isConnected()));
    }

    @Test(timeOut = TIMEOUT)
    public void testAuthenticationFail() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(getMqttBrokerPortList().get(0));
        try {
            client.connectWith()
                    .simpleAuth()
                    .username("user1")
                    .password("pass11".getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth()
                    .send();
        } catch (Mqtt5ConnAckException ex) {
            Mqtt5ConnAckReasonCode reasonCode = ex.getMqttMessage().getReasonCode();
            Assert.assertEquals(reasonCode, Mqtt5ConnAckReasonCode.NOT_AUTHORIZED);
        }
    }

    @Test(timeOut = TIMEOUT)
    public void testAuthenticationSuccess() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connAck = client.connectWith()
                .simpleAuth()
                .username("user1")
                .password("pass1".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .send();
        Assert.assertEquals(connAck.getReasonCode(), Mqtt5ConnAckReasonCode.SUCCESS);
    }
}

