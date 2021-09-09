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
import java.util.UUID;
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

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT)
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
    }

    @Test(expectedExceptions = {EOFException.class, IllegalStateException.class})
    public void testInvalidClientId() throws Exception {
        MQTT mqtt = createMQTTProxyClient();
        mqtt.setConnectAttemptsMax(1);
        // ClientId is invalid, for max length is 23 in mqtt 3.1
        mqtt.setClientId(UUID.randomUUID().toString().replace("-", ""));
        BlockingConnection connection = Mockito.spy(mqtt.blockingConnection());
        connection.connect();
        verify(connection, Mockito.times(2)).connect();
    }

    @Test
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
}
