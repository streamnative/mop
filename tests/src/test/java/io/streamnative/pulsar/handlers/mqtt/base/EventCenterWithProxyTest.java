package io.streamnative.pulsar.handlers.mqtt.base;

import io.streamnative.pulsar.handlers.mqtt.MQTTProtocolHandler;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.event.DisableEventCenter;
import io.streamnative.pulsar.handlers.mqtt.event.PulsarEventCenter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

@Test
public class EventCenterWithProxyTest extends MQTTTestBase {

    @Override
    public MQTTServerConfiguration initConfig() throws Exception {
        MQTTServerConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        return conf;
    }

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
