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
package io.streamnative.pulsar.handlers.mqtt.hivemq;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class HiveMQIntegrationTest extends MQTTTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test(dataProvider = "mqttPersistentTopicNames", timeOut = TIMEOUT)
    public void testBasicPublishAndConsumeWithMQTT(String topic) throws Exception {
        Mqtt3BlockingClient client = createMqtt3Client();
        client.connect();
        client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        try (Mqtt3BlockingClient.Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt3Publish publish = publishes.receive();
            Assert.assertEquals(publish.getPayloadAsBytes(), msg);
        }
        client.unsubscribeWith().topicFilter(topic).send();
        client.disconnect();
    }

    @Test(dataProvider = "mqttTopicNameAndFilter", timeOut = TIMEOUT)
    public void testTopicNameFilter(String topic, String filter) throws Exception {
        Mqtt3BlockingClient client = createMqtt3Client();
        client.connect();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        client.subscribeWith().topicFilter(filter).qos(MqttQos.AT_LEAST_ONCE).send();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        try (Mqtt3BlockingClient.Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt3Publish publish = publishes.receive();
            Assert.assertEquals(publish.getPayloadAsBytes(), msg);
        }
        client.unsubscribeWith().topicFilter(filter).send();
        client.disconnect();
    }

    private Mqtt3BlockingClient createMqtt3Client() {
        return Mqtt3Client.builder()
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
    }
}
