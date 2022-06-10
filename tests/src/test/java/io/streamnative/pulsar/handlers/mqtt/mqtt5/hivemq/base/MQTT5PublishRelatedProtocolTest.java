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
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5PublishRelatedProtocolTest extends MQTTTestBase {

    @Test
    public void testUserProperties() throws Exception {
        final String topic = "testUserProperties";
        Mqtt5BlockingClient client1 = Mqtt5Client.builder()
                .identifier("abc")
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        Mqtt5UserProperties userProperty = Mqtt5UserProperties.builder()
                .add("user-1", "value-1")
                .add("user-2", "value-2")
                .build();
        Mqtt5UserProperty userProperty1 = Mqtt5UserProperty.of("user-1", "value-1");
        Mqtt5UserProperty userProperty2 = Mqtt5UserProperty.of("user-2", "value-2");
        client1.connectWith().send();
        Mqtt5Publish publishMessage = Mqtt5Publish.builder().topic(topic).qos(MqttQos.AT_LEAST_ONCE).userProperties(userProperty).build();

        Mqtt5BlockingClient client2 = Mqtt5Client.builder()
                .identifier( "ccc")
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client2.connectWith().send();
        client2.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL);
        client1.publish(publishMessage);
        Mqtt5Publish message = publishes.receive();
        Assert.assertNotNull(message);
        // Validate the user properties order, must be the same with set order.
        Assert.assertEquals(message.getUserProperties().asList().get(0).compareTo(userProperty1), 0);
        Assert.assertEquals(message.getUserProperties().asList().get(1).compareTo(userProperty2), 0);
        publishes.close();
        client2.unsubscribeWith().topicFilter(topic).send();
        client1.disconnect();
        client2.disconnect();
    }
}
