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
import io.streamnative.pulsar.handlers.mqtt.base.PortManager;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * MQTT proxy related test
 */
@Slf4j
public class ProxyTest extends MQTTTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.setup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void MQTTProxyTest() throws Exception {
        int proxyPort = PortManager.nextFreePort();
        final String topicName = "persistent://public/default/proxy";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", proxyPort);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }
}
