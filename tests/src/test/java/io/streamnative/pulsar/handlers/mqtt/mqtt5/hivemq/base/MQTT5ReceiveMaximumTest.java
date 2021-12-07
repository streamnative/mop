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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectRestrictions;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;



public class MQTT5ReceiveMaximumTest extends MQTTTestBase {


    @Test(timeOut = TIMEOUT)
    public void testExceedReceiveMaximumWillBlock() throws Exception {
        final String topic = "test-receive-maximum-1";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnectRestrictions restrictions = Mqtt5ConnectRestrictions.builder()
                .receiveMaximum(3)
                .build();
        client.connectWith()
                .restrictions(restrictions)
                .send();
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        MQTT5ClientUtils.publishARandomMsg(client, topic);
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL, true)) {
            publishes.receive();
            MQTT5ClientUtils.publishARandomMsg(client, topic);
            publishes.receive();
            MQTT5ClientUtils.publishARandomMsg(client, topic);
            publishes.receive();
            MQTT5ClientUtils.publishARandomMsg(client, topic);
            Optional<Mqtt5Publish> willBlock = publishes.receive(5, TimeUnit.SECONDS);
            Assert.assertFalse(willBlock.isPresent());
        }
        client.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testReceiveMaximumNormalCondition() throws Exception {
        final String topic = "test-receive-maximum-2";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnectRestrictions restrictions = Mqtt5ConnectRestrictions.builder()
                .receiveMaximum(3)
                .build();
        client.connectWith()
                .restrictions(restrictions)
                .send();
        client.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        MQTT5ClientUtils.publishARandomMsg(client, topic);
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL, true)) {
            Mqtt5Publish msg1 = publishes.receive();
            msg1.acknowledge();
            MQTT5ClientUtils.publishARandomMsg(client, topic);
            Mqtt5Publish msg2 = publishes.receive();
            msg2.acknowledge();
            MQTT5ClientUtils.publishARandomMsg(client, topic);
            Mqtt5Publish msg3 = publishes.receive();
            msg3.acknowledge();
            MQTT5ClientUtils.publishARandomMsg(client, topic);
            Optional<Mqtt5Publish> msg4 = publishes.receive(5, TimeUnit.SECONDS);
            Assert.assertTrue(msg4.isPresent());
            msg4.get().acknowledge();
        }
        client.disconnect();
    }

}
