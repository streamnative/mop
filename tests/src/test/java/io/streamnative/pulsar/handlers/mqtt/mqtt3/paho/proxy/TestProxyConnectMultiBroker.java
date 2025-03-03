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

package io.streamnative.pulsar.handlers.mqtt.mqtt3.paho.proxy;

import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
@Slf4j
public class TestProxyConnectMultiBroker extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setDefaultNumberOfNamespaceBundles(4);
        mqtt.setMqttProxyEnabled(true);
        return mqtt;
    }

    public static class Callback implements MqttCallback {

        @Override
        public void connectionLost(Throwable throwable) {
            log.info("Connection lost");
        }

        @Override
        public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
            log.info("Message arrived");
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        }
    }

    @Test(timeOut = 1000 * 60 * 5)
    public void testProxyConnectMultiBroker() throws Exception {
        int port = getMqttProxyPortList().get(0);
        MqttAsyncClient client = new MqttAsyncClient("tcp://localhost:" + port, "test", new MemoryPersistence());
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        connectOptions.setKeepAliveInterval(5);
        log.info("connecting...");
        client.connect(connectOptions).waitForCompletion();
        log.info("connected");

        client.subscribe("testsub1", 1).waitForCompletion();
        log.info("subscribed testsub1");
        // sleep the keep alive period to show that PING will happen in abscence of other messages.
        Thread.sleep(6000);

        // make more subscriptions to connect to multiple brokers.
        client.subscribe("testsub2", 1).waitForCompletion();
        log.info("subscribed testsub2");
        client.subscribe("testsub3", 1).waitForCompletion();
        log.info("subscribed testsub3");
        Map<String, List<String>> msgs = new HashMap<>();
        String topic = "test1";
        client.subscribe(topic, 1, new IMqttMessageListener() {

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                msgs.compute(topic, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(new String(message.getPayload()));
                    return v;
                });
            }
        }).waitForCompletion();

        // publish QoS 1 message to prevent the need for PINGREQ. Keep alive only sends ping in abscence of other
        // messages. Refer to section 3.1.2.10 of the MQTT 3.1.1 specification.
        for (int i = 0; i < 130; i++) {
            log.info("Publishing message..." + System.currentTimeMillis());
            client.publish(topic, "test".getBytes(), 1, false).waitForCompletion();
            Thread.sleep(1000);
        }
        Assert.assertNotNull(msgs.get(topic) != null);
        Assert.assertEquals(msgs.get(topic).size(), 130);
        client.disconnect().waitForCompletion();
    }
}
