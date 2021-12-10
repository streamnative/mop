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
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test user can limit server receive message maximum.
 */
public class MQTT5ServerReceiveMaximumTest extends MQTTTestBase {
    private static final int RECEIVE_MAXIMUM = 1;

    /**
     * init configuration
     * @return MQTTServerConfiguration
     */
    @Override
    protected MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration conf = super.initConfig();
        conf.setReceiveMaximum(RECEIVE_MAXIMUM);
        return conf;
    }

    /**
     * Test client can receive server receive message maximum number configuration.
     */
    @Test(timeOut = TIMEOUT)
    public void testConfigSuccess() throws Exception {
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        Mqtt5ConnAck connect = client.connect();
        int serverReceiveMaximum = connect.getRestrictions().getReceiveMaximum();
        Assert.assertEquals(serverReceiveMaximum, RECEIVE_MAXIMUM);
        client.disconnect();
    }


}
