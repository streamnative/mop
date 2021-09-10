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

package io.streamnative.pulsar.handlers.mqtt;

import static org.mockito.Mockito.verify;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.io.EOFException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration tests for MQTT protocol handler with proxy.
 */
public class ProxyTest extends MQTTTestBase {

    @Override
    protected MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration mqtt = super.initConfig();

        mqtt.setMqttProxyEnable(true);

        return mqtt;
    }

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT, priority = 4)
    public void testSendAndConsume(String topicName) throws Exception {
        MQTT mqtt = createMQTTProxyClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT Proxy";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
        pulsarServiceList.get(0).getBrokerService().forEachTopic(t -> t.delete().join());
    }

    @Test(expectedExceptions = {EOFException.class, IllegalStateException.class}, priority = 3)
    public void testInvalidClientId() throws Exception {
        MQTT mqtt = createMQTTProxyClient();
        mqtt.setConnectAttemptsMax(1);
        // ClientId is invalid, for max length is 23 in mqtt 3.1
        mqtt.setClientId(UUID.randomUUID().toString().replace("-", ""));
        BlockingConnection connection = Mockito.spy(mqtt.blockingConnection());
        connection.connect();
        verify(connection, Mockito.times(2)).connect();
    }

    @Test(timeOut = TIMEOUT, priority = 2)
    public void testSendAndConsumeAcrossProxy() throws Exception {
        int numMessage = 3;
        String topicName = "a/b/c";
        MQTT mqtt0 = new MQTT();
        mqtt0.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection0 = mqtt0.blockingConnection();
        connection0.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection0.subscribe(topics);

        String message = "Hello MQTT Proxy";
        MQTT mqtt1 = new MQTT();
        mqtt1.setHost("127.0.0.1", mqttProxyPortList.get(1));
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        connection1.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        MQTT mqtt2 = new MQTT();
        mqtt2.setHost("127.0.0.1", mqttProxyPortList.get(2));
        BlockingConnection connection2 = mqtt2.blockingConnection();
        connection2.connect();
        connection2.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        MQTT mqtt3 = new MQTT();
        mqtt3.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection3 = mqtt3.blockingConnection();
        connection3.connect();
        connection3.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        for (int i = 0; i < numMessage; i++) {
            Message received = connection0.receive();
            Assert.assertEquals(received.getTopic(), topicName);
            Assert.assertEquals(new String(received.getPayload()), message);
            received.ack();
        }

        connection3.disconnect();
        connection2.disconnect();
        connection1.disconnect();
        connection0.disconnect();
    }

    @Test(dataProvider = "mqttTopicNameAndFilter", timeOut = 30000, priority = 1)
    @SneakyThrows
    public void testSendAndConsumeWithFilter(String topic, String filter) {
        MQTT mqtt0 = createMQTTProxyClient();
        BlockingConnection connection0 = mqtt0.blockingConnection();
        connection0.connect();
        Topic[] topics = { new Topic(filter, QoS.AT_MOST_ONCE) };
        String message = "Hello MQTT Proxy";
        MQTT mqtt1 = createMQTTProxyClient();
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        connection1.publish(topic, message.getBytes(), QoS.AT_MOST_ONCE, false);
        // wait for the publish topic has been stored
        Awaitility.await().untilAsserted(() -> {
                    CompletableFuture<List<String>> listOfTopics = pulsarServiceList.get(0).getNamespaceService()
                    .getListOfTopics(NamespaceName.get("public/default"), CommandGetTopicsOfNamespace.Mode.PERSISTENT);
                    Assert.assertTrue(listOfTopics.join().size() >= 1);
        });
        connection0.subscribe(topics);
        connection1.publish(topic, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection0.receive();
        Assert.assertTrue(new TopicFilterImpl(filter).test(received.getTopic()));
        Assert.assertEquals(new String(received.getPayload()), message);

        connection1.disconnect();
        connection0.disconnect();
    }
}
