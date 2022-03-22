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
package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.proxy;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MQTT5ProxyReasonCodeAckTest extends MQTTTestBase {

    @Override
    public MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        return conf;
    }

    @Test(timeOut = TIMEOUT)
    public void testSameClientIdConnectFail() {
        final String clientId = "1";
        Mqtt5BlockingClient client1 = Mqtt5Client.builder()
                .identifier(clientId)
                .serverHost("127.0.0.1")
                .serverPort(getMqttProxyPortList().get(0))
                .buildBlocking();
        Mqtt5BlockingClient client2 = Mqtt5Client.builder()
                .identifier(clientId)
                .serverHost("127.0.0.1")
                .serverPort(getMqttProxyPortList().get(0))
                .buildBlocking();
        client1.connect();
        try {
            client2.connect();
            Assert.fail("Unexpected reach out");
        } catch (Mqtt5ConnAckException ex) {
            Mqtt5ConnAck mqttMessage = ex.getMqttMessage();
            Assert.assertEquals(mqttMessage.getReasonCode(), Mqtt5ConnAckReasonCode.CLIENT_IDENTIFIER_NOT_VALID);
        }
        client1.disconnect();
    }
}
