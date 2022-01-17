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

import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

/**
 * MQTT proxy related test.
 */
@Slf4j
public class ProxyTest extends MQTTTestBase {
    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.setup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider(name = "mqttTopicNames")
    public Object[][] mqttTopicNames() {
        return new Object[][] {
                { "public/default/t0" },
                { "/public/default/t0" },
                { "public/default/t0/" },
                { "/public/default/t0/" }
        };
    }

    //@Test(dataProvider = "mqttTopicNames", timeOut = 60000)
    public void mqttProxyTest(String topicName) throws Exception {
        setBrokerCount(3);
        int proxyPort = getProxyPort();
        log.info("proxy port value: {}", proxyPort);
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", proxyPort);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT Proxy";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }
}
