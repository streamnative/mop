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
package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5CleanStartTest extends MQTTTestBase {

    @Test(timeOut = TIMEOUT)
    public void testConnectSessionNotPresent() throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connect();
        boolean sessionPresent = connect.isSessionPresent();
        Assert.assertFalse(sessionPresent);
    }

    @Test(timeOut = TIMEOUT)
    public void testConnectSessionPresent() throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connectWith()
                .cleanStart(false)
                .send();
        boolean sessionPresent = connect.isSessionPresent();
        Assert.assertTrue(sessionPresent);
    }
}
