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

import com.hivemq.client.internal.mqtt.message.publish.MqttPublishResult;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5UnsubAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5WillPublish;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.unsuback.Mqtt5UnsubAck;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.unsuback.Mqtt5UnsubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5ReasonCodeOnAllACKTest extends MQTTTestBase {

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testUnSubScribeSuccess(String topic) throws Exception {
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
        Mqtt5UnsubAck ack = client.unsubscribeWith().topicFilter(topic).send();
        for (Mqtt5UnsubAckReasonCode reasonCode : ack.getReasonCodes()) {
            Assert.assertEquals(reasonCode.getCode(), Mqtt5UnsubReasonCode.SUCCESS.value());
        }
        client.disconnect();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testNoSubscribeExisted(String topic) {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        Mqtt5UnsubAck ack = client.unsubscribeWith().topicFilter(topic).send();
        for (Mqtt5UnsubAckReasonCode reasonCode : ack.getReasonCodes()) {
            Assert.assertEquals(reasonCode.getCode(), Mqtt5UnsubAckReasonCode.NO_SUBSCRIPTIONS_EXISTED.getCode());
        }
        client.disconnect();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testTopicFilterInvalid(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        try {
            client.unsubscribeWith().topicFilter(topic).send();
        } catch (Mqtt5UnsubAckException ex) {
            for (Mqtt5UnsubAckReasonCode reasonCode : ex.getMqttMessage().getReasonCodes()) {
                Assert.assertEquals(reasonCode.getCode(), Mqtt5UnsubAckReasonCode.TOPIC_FILTER_INVALID.getCode());
            }
        } finally {
            client.disconnect();
        }
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testSubscribeSuccess(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        Mqtt5SubAck firstConsumerACK = client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
        for (Mqtt5SubAckReasonCode reasonCode : firstConsumerACK.getReasonCodes()) {
            Assert.assertEquals(reasonCode.getCode(), Mqtt5SubAckReasonCode.GRANTED_QOS_1.getCode());
        }
        client.unsubscribeWith().topicFilter(topic).send();
        client.disconnect();
        Mqtt5BlockingClient client2 = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client2.connect();
        Mqtt5SubAck secondConsumerACK = client2.subscribeWith().topicFilter(topic).qos(MqttQos.AT_MOST_ONCE).send();
        for (Mqtt5SubAckReasonCode reasonCode : secondConsumerACK.getReasonCodes()) {
            Assert.assertEquals(reasonCode.getCode(), Mqtt5SubAckReasonCode.GRANTED_QOS_0.getCode());
        }
        client2.unsubscribeWith().topicFilter(topic).send();
        client2.disconnect();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testSubscribeManyTimes(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
        try {
            client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_MOST_ONCE).send();
        } catch (Mqtt5SubAckException ex) {
            Assert.assertTrue(ex.getMqttMessage().getReasonCodes().contains(Mqtt5SubAckReasonCode.UNSPECIFIED_ERROR));
        }
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testPublishSuccessAck(String topic) {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
        final byte[] msg = "hi pulsar".getBytes(StandardCharsets.UTF_8);
        MqttPublishResult.MqttQos1Result publishResult = (MqttPublishResult.MqttQos1Result) client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        Mqtt5PubAckReasonCode reasonCode = publishResult.getPubAck().getReasonCode();
        Assert.assertEquals(reasonCode, Mqtt5PubAckReasonCode.SUCCESS);
        client.disconnect();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testPublishNoMatchingSubscriber(String topic) {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        final byte[] msg = "hi pulsar".getBytes(StandardCharsets.UTF_8);
        MqttPublishResult.MqttQos1Result publishResult = (MqttPublishResult.MqttQos1Result) client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        Mqtt5PubAckReasonCode reasonCode = publishResult.getPubAck().getReasonCode();
        Assert.assertEquals(reasonCode, Mqtt5PubAckReasonCode.NO_MATCHING_SUBSCRIBERS);
        client.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testConnectSuccess() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connect();
        Mqtt5ConnAckReasonCode reasonCode = connect.getReasonCode();
        Assert.assertEquals(reasonCode, Mqtt5ConnAckReasonCode.SUCCESS);
        client.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testUnSupportWillMessage() {
        Mqtt5WillPublish willPublish = Mqtt5Publish.builder()
                .topic("aaa")
                .qos(MqttQos.EXACTLY_ONCE)
                .asWill()
                .build();
        Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .willPublish(willPublish)
                .buildBlocking();
        try {
            client.connect();
        } catch (Mqtt5ConnAckException ex){
            Mqtt5ConnAck mqttMessage = ex.getMqttMessage();
            Mqtt5ConnAckReasonCode reasonCode = mqttMessage.getReasonCode();
            Assert.assertEquals(reasonCode, Mqtt5ConnAckReasonCode.QOS_NOT_SUPPORTED);
        }
    }

    @Test(timeOut = TIMEOUT)
    public void testDisConnectSuccess() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connect();
        Mqtt5ConnAckReasonCode reasonCode = connect.getReasonCode();
        Assert.assertEquals(reasonCode, Mqtt5ConnAckReasonCode.SUCCESS);
        client.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testTopicAliasMaximum() {
        final int aliasMaximum = 2000;
        Mqtt5BlockingClient mqtt5Client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck ack = mqtt5Client.connectWith()
                .restrictions()
                .topicAliasMaximum(aliasMaximum)
                .applyRestrictions()
                .send();
        Assert.assertEquals(ack.getRestrictions().getTopicAliasMaximum(), 2000);
    }
}
