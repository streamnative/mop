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

import io.streamnative.pulsar.handlers.mqtt.MQTTProtocolHandler;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.event.DisableEventCenter;
import io.streamnative.pulsar.handlers.mqtt.event.PulsarEventCenter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class EventCenterWithProxyTest extends MQTTTestBase {

    @Override
    public MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        return conf;
    }

    @Test
    public void testDisableBrokerEventCenter() {
        List<PulsarService> pulsarServiceList = getPulsarServiceList();
        PulsarService pulsarService = pulsarServiceList.get(0);
        ProtocolHandler mqtt = pulsarService.getProtocolHandlers().protocol("mqtt");
        MQTTProtocolHandler protocolHandler = (MQTTProtocolHandler) mqtt;
        PulsarEventCenter eventCenter = protocolHandler.getMqttService().getEventCenter();
        Assert.assertTrue(eventCenter instanceof DisableEventCenter);
        try {
            eventCenter.register(null);
            Assert.fail("Unexpected result");
        } catch (UnsupportedOperationException ex) {
            // expected result
        }
        try {
            eventCenter.unRegister(null);
            Assert.fail("Unexpected result");
        } catch (UnsupportedOperationException ex) {
            // expected result
        }
        try {
            eventCenter.shutdown();
            Assert.fail("Unexpected result");
        } catch (UnsupportedOperationException ex) {
            // expected result
        }
    }
}
