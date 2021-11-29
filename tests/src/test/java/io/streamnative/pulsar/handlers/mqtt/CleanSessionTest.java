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

import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.UUID;

@Slf4j
public class CleanSessionTest extends MQTTTestBase {

    @Test(timeOut = TIMEOUT)
    public void testConnectSessionNotPresent() throws Exception {
        Mqtt3BlockingClient client = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        Mqtt3ConnAck connect = client.connect();
        boolean sessionPresent = connect.isSessionPresent();
        Assert.assertFalse(sessionPresent);
    }

    @Test(timeOut = TIMEOUT)
    public void testConnectSessionPresent() throws Exception {
        Mqtt3BlockingClient client = Mqtt3Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        Mqtt3ConnAck connect = client.connectWith()
                .cleanSession(false)
                .send();
        boolean sessionPresent = connect.isSessionPresent();
        Assert.assertTrue(sessionPresent);
    }

}
