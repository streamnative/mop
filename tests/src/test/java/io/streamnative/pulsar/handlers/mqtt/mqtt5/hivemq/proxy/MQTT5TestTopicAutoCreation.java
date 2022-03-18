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
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5SubAckException;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base.MQTT5ClientUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Random;


public class MQTT5TestTopicAutoCreation extends MQTTTestBase {
    private final Random random = new Random();

    @Override
    protected MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration mqttServerConfiguration = super.initConfig();
        mqttServerConfiguration.setAllowAutoTopicCreation(false);
        mqttServerConfiguration.setMqttProxyEnabled(true);
        return mqttServerConfiguration;
    }

    @Test(timeOut = TIMEOUT)
    public void testPublishNotAllowTopicAutoCreation() {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        client.connect();
        byte[] msg = "payload".getBytes();
        try {
            client.publishWith()
                    .topic("proxy/topic-not-allow-auto-creation-publish")
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
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5ProxyClient(
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        client.connect();
        try {
            client.subscribeWith()
                    .topicFilter("proxy/topic-not-allow-auto-creation-subscribe")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .send();
        } catch (Mqtt5SubAckException ex) {
            Assert.assertTrue(ex.getMqttMessage().getReasonCodes().contains(Mqtt5SubAckReasonCode.UNSPECIFIED_ERROR));
            Assert.assertTrue(ex.getMqttMessage().getReasonString().isPresent());
            Assert.assertEquals(ex.getMqttMessage().getReasonString().get().toString(), "Topic not found");
        }
    }
}
