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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectRestrictions;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.base.AuthorizationConfig;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base.MQTT5ClientUtils;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5AuthorizationProxyReasonCodeOnAllAckTest extends AuthorizationConfig {
    private final Random random = new Random();

    @Override
    public MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration conf = super.initConfig();
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
            Assert.fail();
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
            Assert.fail();
        } catch (Mqtt5PubAckException ex) {
            Mqtt5PubAckReasonCode reasonCode = ex.getMqttMessage().getReasonCode();
            Assert.assertEquals(reasonCode, Mqtt5PubAckReasonCode.NOT_AUTHORIZED);
            Assert.assertTrue(ex.getMqttMessage().getReasonString().isPresent());
        }
        Awaitility.await()
                .untilAsserted(() -> Assert.assertFalse(client.getState().isConnected()));
    }

    @Test(timeOut = TIMEOUT)
    public void testPublishWithRequestProblemInformation() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(getMqttBrokerPortList().get(0));
        client.connectWith()
                .simpleAuth()
                .username("user1")
                .password("pass1".getBytes(StandardCharsets.UTF_8))
                .applySimpleAuth()
                .restrictions(Mqtt5ConnectRestrictions.builder()
                        .requestProblemInformation(false)
                        .build())
                .send();
        String message = "Hello MQTT Pulsar";
        try {
            client.publishWith()
                    .topic("a")
                    .payload(message.getBytes(StandardCharsets.UTF_8))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send();
            client.subscribeWith().topicFilter("a").qos(MqttQos.AT_LEAST_ONCE).send();
            Assert.fail();
        } catch (Mqtt5PubAckException ex) {
            Mqtt5PubAckReasonCode reasonCode = ex.getMqttMessage().getReasonCode();
            Assert.assertEquals(reasonCode, Mqtt5PubAckReasonCode.NOT_AUTHORIZED);
            Assert.assertFalse(ex.getMqttMessage().getReasonString().isPresent());
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
            Assert.assertEquals(reasonCode, Mqtt5ConnAckReasonCode.BAD_USER_NAME_OR_PASSWORD);
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

    @Test
    public void testPublishWithUserPropertiesAndEnableAuthorization() throws Exception {
        Set<AuthAction> userActions = new HashSet<>();
        userActions.add(AuthAction.produce);
        userActions.add(AuthAction.consume);
        admin.namespaces().grantPermissionOnNamespace("public/default", "user1", userActions);


        final String topic = "testPublishWithUserProperties";
        Mqtt5BlockingClient client1 = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        Mqtt5UserProperties userProperty = Mqtt5UserProperties.builder()
            .add("user-1", "value-1")
            .add("user-2", "value-2")
            .build();
        Mqtt5UserProperty userProperty1 = Mqtt5UserProperty.of("user-1", "value-1");
        Mqtt5UserProperty userProperty2 = Mqtt5UserProperty.of("user-2", "value-2");
        client1.connectWith()
            .simpleAuth()
            .username("user1")
            .password("pass1".getBytes(StandardCharsets.UTF_8))
            .applySimpleAuth()
            .send();
        Mqtt5Publish publishMessage = Mqtt5Publish.builder().topic(topic).qos(MqttQos.AT_LEAST_ONCE)
            .userProperties(userProperty)
            .asWill().build();

        Mqtt5BlockingClient client2 = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        client2.connectWith()
            .simpleAuth()
            .username("user1")
            .password("pass1".getBytes(StandardCharsets.UTF_8))
            .applySimpleAuth()
            .send();
        client2.subscribeWith()
            .topicFilter(topic)
            .qos(MqttQos.AT_LEAST_ONCE)
            .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL);
        client1.publish(publishMessage);
        Mqtt5Publish message = publishes.receive();
        Assert.assertNotNull(message);
        log.info("Received message properties: {}", message.getUserProperties());
        // Validate the user properties order, must be the same with set order.
        Assert.assertEquals(message.getUserProperties().asList().get(0).compareTo(userProperty1), 0);
        Assert.assertEquals(message.getUserProperties().asList().get(1).compareTo(userProperty2), 0);
        publishes.close();
        client2.unsubscribeWith().topicFilter(topic).send();
        client1.disconnect();
        client2.disconnect();
    }
}

