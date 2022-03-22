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
package io.streamnative.pulsar.handlers.mqtt.mqtt3.hivemq.base;

import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.exceptions.Mqtt3ConnAckException;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MQTT3ReasonCodeTest extends MQTTTestBase {

    @Test(timeOut = TIMEOUT)
    public void testSameClientIdConnectFail() {
        final String clientId = "1";
        Mqtt3BlockingClient client1 = Mqtt3Client.builder()
                .identifier(clientId)
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        Mqtt3BlockingClient client2 = Mqtt3Client.builder()
                .identifier(clientId)
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client1.connect();
        try {
            client2.connect();
            Assert.fail("Unexpected reach out");
        } catch (Mqtt3ConnAckException ex) {
            Mqtt3ConnAck mqttMessage = ex.getMqttMessage();
            Assert.assertEquals(mqttMessage.getReturnCode(), Mqtt3ConnAckReturnCode.IDENTIFIER_REJECTED);
        }
        client1.disconnect();
    }
}
