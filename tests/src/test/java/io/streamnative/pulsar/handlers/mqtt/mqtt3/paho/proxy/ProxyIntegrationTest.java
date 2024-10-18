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

import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ProxyIntegrationTest extends MQTTTestBase {
    private static class Callback implements MqttCallback {

        @Override
        public void connectionLost(java.lang.Throwable throwable) {
            throw new RuntimeException("MQTT Client connection died");
        }

        @Override
        public void messageArrived(java.lang.String s, MqttMessage mqttMessage) throws Exception {
            System.out.println("Message arrived");
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        }
    }

    private final Random random = new Random();

    @Override
    public MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration conf = super.initConfig();
        conf.setMqttProxyEnabled(true);
        conf.getProperties().setProperty("systemEventEnabled", "false");
        conf.setSystemTopicEnabled(false);
        return conf;
    }

    @Test
    public void testProxyKeepAlivePaho() throws Exception {
        String topicPrefix = "topic";
        Set<String> brokers = new HashSet<>();
        List<String> topics = new ArrayList<>();
        for (int i = 1; i <= 2000; i++) {
            String topic = topicPrefix + i;
            final String ownedBroker = admin.lookups().lookupTopic(topic);
            boolean added = brokers.add(ownedBroker);
            if (added) {
                topics.add(topic);
                if (brokers.size() == 3) {
                    break;
                }
            }
        }
        String serverUri = String.format("tcp://127.0.0.1:%d",
                getMqttProxyPortList().get(random.nextInt(mqttProxyPortList.size())));
        MqttAsyncClient client = new MqttAsyncClient(serverUri, "test", new MemoryPersistence());
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        connectOptions.setKeepAliveInterval(5);
        System.out.println("connecting...");
        client.connect(connectOptions).waitForCompletion();
        System.out.println("connected");


        // Another bug, you need to make a sub or pub before the MoP proxy will even respond to pings, because you don't
        // have any adapter channels open to forward PINGREQ messages to.
        client.subscribe(topics.get(0), 1).waitForCompletion();

        // sleep the keep alive period to show that PING will happen in abscence of other messages.
        Thread.sleep(6000);

        // make more subscriptions to connect to multiple brokers.
        client.subscribe(topics.get(1), 1).waitForCompletion();
        client.subscribe(topics.get(2), 1).waitForCompletion();

        // assert client is connected before publishing 10 messages.
        Assert.assertTrue(client.isConnected());

        // publish QoS 1 message to prevent the need for PINGREQ. Keep alive only sends ping in abscence of other
        // messages. Refer to section 3.1.2.10 of the MQTT 3.1.1 specification.
        int i = 0;
        try {
            while (i < 10) {
                System.out.println("Publishing message...");
                client.publish(topics.get(0), "test".getBytes(), 1, false).waitForCompletion();
                Thread.sleep(1000);
                i++;
            }
        } catch (MqttException e){
            // an exception will be thrown, as the client will disconnect
            System.out.println("MQTT Exception: " + e.getMessage());
        } finally {
            // after publishing 10 messages with a one-second delay, the client should still be connected.
            Assert.assertTrue(client.isConnected(), "Client should be connected");
        }
    }
}
