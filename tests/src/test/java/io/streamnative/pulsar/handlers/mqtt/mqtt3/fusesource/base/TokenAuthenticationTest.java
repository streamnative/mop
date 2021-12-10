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
package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.base;

import io.streamnative.pulsar.handlers.mqtt.base.TokenAuthenticationConfig;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.MQTTException;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Token authentication integration tests for MQTT protocol handler.
 */
@Slf4j
public class TokenAuthenticationTest extends TokenAuthenticationConfig {

    @Test(timeOut = TIMEOUT)
    public void testAuthenticate() throws Exception {
        MQTT mqtt = createMQTTClient();
        String topicName = "persistent://public/default/testAuthentication";
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = {new Topic(topicName, QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test(expectedExceptions = {MQTTException.class})
    public void testInvalidCredentials() throws Exception {
        MQTT mqtt = createMQTTClient();
        mqtt.setPassword("invalid");
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }
}
