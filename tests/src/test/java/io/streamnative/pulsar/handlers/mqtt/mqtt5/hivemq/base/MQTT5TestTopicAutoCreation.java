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
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MQTT5TestTopicAutoCreation extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqttServerConfiguration = super.initConfig();
        mqttServerConfiguration.setAllowAutoTopicCreation(false);
        return mqttServerConfiguration;
    }

    @Test(timeOut = TIMEOUT)
    public void testPublishNotAllowTopicAutoCreation() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        byte[] msg = "payload".getBytes();
        try {
            client.publishWith()
                    .topic("topic-not-allow-auto-creation-publish")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload(msg)
                    .send();
        } catch (Mqtt5PubAckException ex) {
            Assert.assertEquals(ex.getMqttMessage().getReasonCode(), Mqtt5PubAckReasonCode.UNSPECIFIED_ERROR);
            Assert.assertTrue(ex.getMqttMessage().getReasonString().isPresent());
            Assert.assertEquals(ex.getMqttMessage().getReasonString().get().toString(), "Topic not found");
        }
    }

    @Test(timeOut = TIMEOUT)
    public void testSubscribeNotAllowTopicAutoCreation() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        try {
            client.subscribeWith()
                    .topicFilter("topic-not-allow-auto-creation-subscribe")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send();
        } catch (Mqtt5SubAckException ex) {
            Assert.assertTrue(ex.getMqttMessage().getReasonCodes().contains(Mqtt5SubAckReasonCode.UNSPECIFIED_ERROR));
            Assert.assertTrue(ex.getMqttMessage().getReasonString().isPresent());
            Assert.assertEquals(ex.getMqttMessage().getReasonString().get().toString(), "Topic not found");
        }
    }

    @Test(timeOut = TIMEOUT)
    public void testPublishAllowTopicCreationByNamespacePolicy() throws PulsarAdminException {
        AutoTopicCreationOverride param = AutoTopicCreationOverride.builder()
                .topicType(TopicType.NON_PARTITIONED.toString())
                .allowAutoTopicCreation(true).build();
        admin.namespaces().setAutoTopicCreation("public/default", param);

        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        byte[] msg = "payload".getBytes();
        try {
            client.publishWith()
                    .topic("topic-not-allow-auto-creation-publish")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload(msg)
                    .send();
        } catch (Mqtt5PubAckException ex) {
            Assert.fail("Unexpected result");
        }
        client.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testSubscribeAllowTopicCreationByNamespacePolicy() throws PulsarAdminException {
        AutoTopicCreationOverride param = AutoTopicCreationOverride.builder()
                .topicType(TopicType.NON_PARTITIONED.toString())
                .allowAutoTopicCreation(true).build();
        admin.namespaces().setAutoTopicCreation("public/default", param);

        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connect();
        try {
            client.subscribeWith()
                    .topicFilter("topic-not-allow-auto-creation-subscribe")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send();
        } catch (Mqtt5SubAckException ex) {
            Assert.fail("Unexpected result");
        }
        client.disconnect();
    }
}
