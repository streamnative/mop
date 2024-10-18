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

package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.proxy;

import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProxyToPulsarTlsTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setMqttProxyEnabled(true);
        mqtt.setTlsEnabled(true);
        mqtt.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        mqtt.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
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
    }

}
