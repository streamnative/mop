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

import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.MQTTException;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Simple integration tests for MQTT protocol handler.
 */
@Slf4j
public class SimpleIntegrationTest extends MQTTTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider(name = "batchEnabled")
    public Object[][] batchEnabled() {
        return new Object[][] {
                { true },
                { false }
        };
    }

    @Test
    public void testSimpleMqttPubAndSubQos0() throws Exception {
        final String topicName = "persistent://public/default/qos0";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test
    public void testSimpleMqttPubAndSubQos1() throws Exception {
        final String topicName = "persistent://public/default/qos1";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test
    public void testSendByMqttAndReceiveByPulsar() throws Exception {
        final String topicName = "persistent://public/default/testReceiveByPulsar";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe();

        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);

        org.apache.pulsar.client.api.Message<byte[]> received = consumer.receive();
        Assert.assertNotNull(received);
        Assert.assertEquals(new String(received.getValue()), message);
        consumer.acknowledge(received);

        consumer.close();
        connection.disconnect();
    }

    @Test(dataProvider = "batchEnabled")
    public void testSendByPulsarAndReceiveByMqtt(boolean batchEnabled) throws Exception {
        final String topicName = "persistent://public/default/testSendByPulsarAndReceiveByMqtt";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(batchEnabled)
                .create();

        String message = "Hello MQTT";

        producer.newMessage().value(message).sendAsync();
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
        producer.close();
    }

    @Test
    public void testBacklogShouldBeZeroWithQos0() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos0";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        int messages = 10000;
        for (int i = 0; i < messages; i++) {
            connection.publish(topicName, (message + i).getBytes(), QoS.AT_MOST_ONCE, false);
        }

        for (int i = 0; i < messages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(new String(received.getPayload()), (message + i));
        }

        Assert.assertEquals(admin.topics().getStats(topicName).subscriptions.size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .subscriptions.entrySet().iterator().next().getValue().msgBacklog, 0);
        connection.disconnect();
    }

    @Test
    public void testBacklogShouldBeZeroWithQos1() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos1";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        int messages = 10000;
        for (int i = 0; i < messages; i++) {
            connection.publish(topicName, (message + i).getBytes(), QoS.AT_LEAST_ONCE, false);
        }

        for (int i = 0; i < messages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(new String(received.getPayload()), (message + i));
            received.ack();
        }

        Thread.sleep(1000);
        Assert.assertEquals(admin.topics().getStats(topicName).subscriptions.size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .subscriptions.entrySet().iterator().next().getValue().msgBacklog, 0);
        connection.disconnect();
    }

    @Test
    public void testBacklogShouldBeZeroWithQos0AndSendByPulsar() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos0AndSendByPulsar-";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        int messages = 10000;
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .blockIfQueueFull(true)
                .enableBatching(false)
                .create();
        for (int i = 0; i < messages; i++) {
            producer.sendAsync(message + i);
        }

        for (int i = 0; i < messages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(new String(received.getPayload()), (message + i));
        }

        Assert.assertEquals(admin.topics().getStats(topicName).subscriptions.size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .subscriptions.entrySet().iterator().next().getValue().msgBacklog, 0);
        connection.disconnect();
    }

    @Test
    public void testBacklogShouldBeZeroWithQos1AndSendByPulsar() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos1AndSendByPulsar";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        int messages = 10000;
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .blockIfQueueFull(true)
                .enableBatching(false)
                .create();
        for (int i = 0; i < messages; i++) {
            producer.sendAsync(message + i);
        }

        for (int i = 0; i < messages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(new String(received.getPayload()), (message + i));
            received.ack();
        }

        Assert.assertEquals(admin.topics().getStats(topicName).subscriptions.size(), 1);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(admin.topics().getStats(topicName)
                .subscriptions.entrySet().iterator().next().getValue().msgBacklog, 0));
        connection.disconnect();
    }

    @Test
    public void testSubscribeRejectionWithSameClientId() throws Exception {
        final String topicName = "persistent://public/default/testSubscribeWithSameClientId";
        MQTT mqtt = new MQTT();
        mqtt.setClientId("my-client-id");
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection1 = mqtt.blockingConnection();
        connection1.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection1.subscribe(topics);

        Assert.assertTrue(connection1.isConnected());

        BlockingConnection connection2;
        try {
            connection2 = mqtt.blockingConnection();
            connection2.connect();
            connection2.subscribe(topics);
            Assert.fail("Should failed with CONNECTION_REFUSED_IDENTIFIER_REJECTED");
        } catch (MQTTException e){
            Assert.assertTrue(e.getMessage().contains("CONNECTION_REFUSED_IDENTIFIER_REJECTED"));
        }
    }

    @Test
    public void testSubscribeWithSameClientId() throws Exception {
        final String topicName = "persistent://public/default/testSubscribeWithSameClientId";
        MQTT mqtt = new MQTT();
        mqtt.setClientId("my-client-id");
        mqtt.setHost("127.0.0.1", getMqttBrokerPortList().get(0));
        BlockingConnection connection1 = mqtt.blockingConnection();
        connection1.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection1.subscribe(topics);

        Assert.assertTrue(connection1.isConnected());
        connection1.disconnect();

        Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertFalse(connection1.isConnected()));

        BlockingConnection connection2 = mqtt.blockingConnection();
        connection2.connect();
        connection2.subscribe(topics);

        Assert.assertTrue(connection2.isConnected());
    }
}
