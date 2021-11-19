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
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5UnsubAckException;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.unsuback.Mqtt5UnsubAck;
import com.hivemq.client.mqtt.mqtt5.message.unsubscribe.unsuback.Mqtt5UnsubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttUnsubAckReasonCode;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5ReasonCodeOnAllACKTest extends MQTTTestBase {
    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testUnSubScribeSuccess(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientFactory.createMqtt5Client(getMqttBrokerPortList().get(0));
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
            Assert.assertEquals(reasonCode.getCode(), MqttUnsubAckReasonCode.SUCCESS.value());
        }
        client.disconnect();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testNoSubscribeExisted(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientFactory.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        Mqtt5UnsubAck ack = client.unsubscribeWith().topicFilter(topic).send();
        for (Mqtt5UnsubAckReasonCode reasonCode : ack.getReasonCodes()) {
            Assert.assertEquals(reasonCode.getCode(), MqttUnsubAckReasonCode.NO_SUBSCRIPTION_EXISTED.value());
        }
        client.disconnect();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testTopicFilterInvalid(String topic) throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientFactory.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        try {
            client.unsubscribeWith().topicFilter(topic).send();
        } catch (Mqtt5UnsubAckException ex) {
            for (Mqtt5UnsubAckReasonCode reasonCode : ex.getMqttMessage().getReasonCodes()) {
                Assert.assertEquals(reasonCode.getCode(), MqttUnsubAckReasonCode.TOPIC_FILTER_INVALID.value());
            }
        } finally {
            client.disconnect();
        }
    }

}
