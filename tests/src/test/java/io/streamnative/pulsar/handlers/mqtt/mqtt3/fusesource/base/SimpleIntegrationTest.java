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
package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.base;

import static org.mockito.Mockito.verify;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.psk.PSKClient;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Simple integration tests for MQTT protocol handler.
 */
@Slf4j
public class SimpleIntegrationTest extends MQTTTestBase {

    private final int numMessages = 10000;

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();

        mqtt.setMqttTlsPskEnabled(true);
        mqtt.setMqttTlsPskIdentityHint("alpha");
        mqtt.setMqttTlsPskIdentity("mqtt:mqtt123");
        return mqtt;
    }

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT)
    public void testSimpleMqttPubAndSubQos0(String topicName) throws Exception {
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT)
    public void testSimpleMqttPubAndSubQos1(String topicName) throws Exception {
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT)
    public void testSendByMqttAndReceiveByPulsar(String topic) throws Exception {
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(PulsarTopicUtils.getEncodedPulsarTopicName(topic, "public", "default", TopicDomain.persistent))
                .subscriptionName("my-sub")
                .subscribe();

        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        String message = "Hello MQTT";

        connection.publish(topic, message.getBytes(), QoS.AT_LEAST_ONCE, false);

        org.apache.pulsar.client.api.Message<byte[]> received = consumer.receive();
        Assert.assertNotNull(received);
        Assert.assertEquals(new String(received.getValue()), message);
        consumer.acknowledge(received);

        consumer.close();
        connection.disconnect();
    }

    @Test(dataProvider = "batchEnabled", timeOut = TIMEOUT)
    public void testSendByPulsarAndReceiveByMqtt(boolean batchEnabled) throws Exception {
        final String topicName = "persistent://public/default/testSendByPulsarAndReceiveByMqtt";
        MQTT mqtt = createMQTTClient();
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
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
        producer.close();
    }

    @Test(timeOut = TIMEOUT)
    public void testBacklogShouldBeZeroWithQos0() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos0";
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        int numMessages = 200;
        for (int i = 0; i < numMessages; i++) {
            connection.publish(topicName, (message + i).getBytes(), QoS.AT_MOST_ONCE, false);
        }

        for (int i = 0; i < numMessages; i++) {
            Message received = connection.receive();
            if (received != null) {
                Assert.assertEquals(new String(received.getPayload()), (message + i));
            }
        }
        Assert.assertEquals(admin.topics().getStats(topicName).getSubscriptions().size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().entrySet().iterator().next().getValue().getMsgBacklog(), 0);
        connection.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testBacklogShouldBeZeroWithQos1() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos1";
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        for (int i = 0; i < numMessages; i++) {
            connection.publish(topicName, (message + i).getBytes(), QoS.AT_LEAST_ONCE, false);
        }

        for (int i = 0; i < numMessages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(new String(received.getPayload()), (message + i));
            received.ack();
        }

        Thread.sleep(1000);
        Assert.assertEquals(admin.topics().getStats(topicName).getSubscriptions().size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().entrySet().iterator().next().getValue().getMsgBacklog(), 0);
        connection.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testBacklogShouldBeZeroWithQos0AndSendByPulsar() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos0AndSendByPulsar";
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .blockIfQueueFull(true)
                .enableBatching(false)
                .create();
        for (int i = 0; i < numMessages; i++) {
            producer.sendAsync(message + i);
        }

        for (int i = 0; i < numMessages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(new String(received.getPayload()), (message + i));
        }

        Assert.assertEquals(admin.topics().getStats(topicName).getSubscriptions().size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().entrySet().iterator().next().getValue().getMsgBacklog(), 0);
        connection.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testBacklogShouldBeZeroWithQos1AndSendByPulsar() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos1AndSendByPulsar";
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .blockIfQueueFull(true)
                .enableBatching(false)
                .create();
        for (int i = 0; i < numMessages; i++) {
            producer.sendAsync(message + i);
        }

        for (int i = 0; i < numMessages; i++) {
            Message received = connection.receive();
            Assert.assertEquals(received.getTopic(), topicName);
            Assert.assertEquals(new String(received.getPayload()), (message + i));
            received.ack();
        }

        Assert.assertEquals(admin.topics().getStats(topicName).getSubscriptions().size(), 1);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).untilAsserted(() ->
                Assert.assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().entrySet().iterator().next().getValue().getMsgBacklog(), 0));
        connection.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testSubscribeRejectionWithSameClientId() throws Exception {
        final String topicName = "persistent://public/default/testSubscribeRejectionWithSameClientId";
        MQTT mqtt = createMQTTClient();
        mqtt.setClientId("client-id-0");
        mqtt.setReconnectDelay(Integer.MAX_VALUE);
        mqtt.setReconnectAttemptsMax(0);
        BlockingConnection connection1 = mqtt.blockingConnection();
        connection1.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection1.subscribe(topics);
        Assert.assertTrue(connection1.isConnected());

        BlockingConnection connection2;
        MQTT mqtt2 = createMQTTClient();
        mqtt2.setClientId("client-id-0");
        connection2 = mqtt2.blockingConnection();
        connection2.connect();
        Assert.assertTrue(connection2.isConnected());
        Awaitility.await().untilAsserted(() -> Assert.assertFalse(connection1.isConnected()));
        connection2.subscribe(topics);
        Assert.assertTrue(connection2.isConnected());
        connection2.disconnect();
        Awaitility.await().untilAsserted(()-> {
            TopicStats stats = admin.topics().getStats(topicName);
            Assert.assertEquals(stats.getSubscriptions().size(), 0);
        });
        connection1.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testSubscribeWithSameClientId() throws Exception {
        final String topicName = "persistent://public/default/testSubscribeWithSameClientId";
        MQTT mqtt = createMQTTClient();
        mqtt.setClientId("client-id-1");
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

        connection2.disconnect();
    }

    @Test
    public void testSubscribeWithTopicFilter() throws Exception {
        String t1 = "a/b/c";
        String t2 = "a/b/c/d";

        MQTT mqtt0 = createMQTTClient();
        BlockingConnection connection0 = mqtt0.blockingConnection();
        connection0.connect();
        Topic[] topics0 = { new Topic(t1, QoS.AT_LEAST_ONCE), new Topic(t2, QoS.AT_LEAST_ONCE) };
        connection0.subscribe(topics0);

        byte[] message = "Hello MQTT Proxy".getBytes(StandardCharsets.UTF_8);
        connection0.publish(t1, message, QoS.AT_MOST_ONCE, false);
        connection0.publish(t2, message, QoS.AT_MOST_ONCE, false);
        Message received = connection0.receive();
        Assert.assertEquals(received.getPayload(), message);
        received = connection0.receive();
        Assert.assertEquals(received.getPayload(), message);

        MQTT mqtt1 = createMQTTClient();
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        Topic[] topics1 = { new Topic("a/b/#", QoS.AT_LEAST_ONCE)};
        connection1.subscribe(topics1);
        connection1.publish(t1, message, QoS.AT_MOST_ONCE, false);
        connection1.publish(t2, message, QoS.AT_MOST_ONCE, false);
        received = connection1.receive();
        Assert.assertEquals(received.getPayload(), message);
        received = connection1.receive();
        Assert.assertEquals(received.getPayload(), message);
        connection0.disconnect();
        connection1.disconnect();

        MQTT mqtt2 = createMQTTClient();
        BlockingConnection connection2 = mqtt2.blockingConnection();
        connection2.connect();
        Topic[] topics2 = { new Topic("a/+/c", QoS.AT_LEAST_ONCE), new Topic("a/+/c/#", QoS.AT_LEAST_ONCE)};
        connection2.subscribe(topics2);
        connection2.publish(t1, message, QoS.AT_MOST_ONCE, false);
        connection2.publish(t2, message, QoS.AT_MOST_ONCE, false);
        received = connection2.receive();
        Assert.assertEquals(received.getPayload(), message);
        received = connection2.receive();
        Assert.assertEquals(received.getPayload(), message);
        connection2.disconnect();
    }

    @Test(expectedExceptions = {EOFException.class, IllegalStateException.class})
    public void testInvalidClientId() throws Exception {
        MQTT mqtt = createMQTTClient();
        mqtt.setConnectAttemptsMax(1);
        // ClientId is invalid, for max length is 23 in mqtt 3.1
        mqtt.setClientId(UUID.randomUUID().toString().replace("-", ""));
        BlockingConnection connection = Mockito.spy(mqtt.blockingConnection());
        connection.connect();
        verify(connection, Mockito.times(2)).connect();
    }

    @Test
    @SneakyThrows
    public void testTlsPskWithTlsv1() {
        Bootstrap client = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        client.group(group);
        client.channel(NioSocketChannel.class);
        client.handler(new PSKClient("alpha", "mqtt", "mqtt123"));
        AtomicBoolean connected = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        client.connect("localhost", mqttBrokerPortTlsPskList.get(0)).addListener((ChannelFutureListener) future -> {
            connected.set(future.isSuccess());
            latch.countDown();
        });
        latch.await();
        Assert.assertTrue(connected.get());
    }

    @Test
    @SneakyThrows
    public void testServlet() {
        HttpClient httpClient = HttpClientBuilder.create().build();
        final String mopEndPoint = "http://localhost:" + brokerWebservicePortList.get(0) + "/mop/stats";
        HttpResponse response = httpClient.execute(new HttpGet(mopEndPoint));
        InputStream inputStream = response.getEntity().getContent();
        InputStreamReader isReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer buffer = new StringBuffer();
        String str;
        while ((str = reader.readLine()) != null){
            buffer.append(str);
        }
        Assert.assertTrue(buffer.toString().contains("active"));
        Assert.assertTrue(buffer.toString().contains("active_clients"));
    }

    @Test
    @SneakyThrows
    public void testConsumerDisconnectNotMissingMessage() {
        String topic = "disconnectNotMissingMessage";
        int total = 1000;
        AtomicInteger receivedCount = new AtomicInteger(0);
        String msgPrefix = "Hello MQTT Proxy- ";
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(1);
        Thread consumerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    int segment = 100;
                    int recCount = 0;
                    int start = 0;
                    MQTT mqttConsumer = createMQTTClient();
                    mqttConsumer.setCleanSession(false);
                    mqttConsumer.setClientId("theSameClientId");
                    for (int l = 0; l < total / segment; l++) {
                        BlockingConnection consumer = mqttConsumer.blockingConnection();
                        consumer.connect();
                        Topic[] topics = { new Topic(topic, QoS.AT_LEAST_ONCE)};
                        consumer.subscribe(topics);
                        latch.countDown();
                        for (int i = start; i < segment + start; i++) {
                            Message received = consumer.receive();
                            received.ack();
                            recCount++;
                            receivedCount.incrementAndGet();
                        }
                        start = recCount;
                        consumer.disconnect();
                    }
                    Assert.assertEquals(start, total);
                    stop.countDown();
                } catch (Throwable ex) {
                    log.error("consumer error", ex);
                }
            }
        });
        consumerThread.start();
        MQTT mqttProducer = createMQTTClient();
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        latch.await();
        for (int i = 0; i < total; i++) {
            byte[] message = (msgPrefix + i).getBytes(StandardCharsets.UTF_8);
            producer.publish(topic, message, QoS.AT_MOST_ONCE, false);
        }
        producer.disconnect();
        stop.await(5, TimeUnit.MINUTES);
        Assert.assertEquals(receivedCount.get(), total);
    }

    @Test
    @SneakyThrows
    public void testCleanSession() {
        String topic = "cleanSession";
        Topic[] topics = { new Topic(topic, QoS.AT_LEAST_ONCE)};
        MQTT mqttConsumer = createMQTTClient();
        mqttConsumer.setClientId("keepTheSameClientId");
        mqttConsumer.setConnectAttemptsMax(0);
        mqttConsumer.setReconnectAttemptsMax(0);
        mqttConsumer.setKeepAlive((short) 3);
        mqttConsumer.setCleanSession(true);
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        consumer.subscribe(topics);
        // Producer
        MQTT mqttProducer = createMQTTClient();
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        producer.publish(topic, "Hello MQTT".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        consumer.receive();
        consumer.suspend();
        Thread.sleep(3000 * 2); // Sleep 2 times of setKeepAlive.
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(pulsarServiceList.get(0).getAdminClient().topics().getSubscriptions(topic).size(), 0);
        });
    }

    @Test(priority = -1)
    public void testConnectionWithSameClientId() throws Exception {
        MQTT mqttProducer = createMQTTClient();
        mqttProducer.setClientId("client1");
        mqttProducer.setUserName("clientUser1");
        mqttProducer.setConnectAttemptsMax(0);
        mqttProducer.setReconnectAttemptsMax(0);
        BlockingConnection producer1 = mqttProducer.blockingConnection();
        producer1.connect();
        // Producer
        MQTT mqttProducer2 = createMQTTClient();
        mqttProducer2.setClientId("client1");
        mqttProducer2.setUserName("clientUser2");
        mqttProducer2.setConnectAttemptsMax(0);
        mqttProducer2.setReconnectAttemptsMax(0);
        BlockingConnection producer2 = mqttProducer2.blockingConnection();
        producer2.connect();
        //
        HttpClient httpClient = HttpClientBuilder.create().build();
        final String mopEndPoint = "http://localhost:" + brokerWebservicePortList.get(0) + "/mop/stats";
        HttpResponse response = httpClient.execute(new HttpGet(mopEndPoint));
        InputStream inputStream = response.getEntity().getContent();
        InputStreamReader isReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer buffer = new StringBuffer();
        String str;
        while ((str = reader.readLine()) != null){
            buffer.append(str);
        }
        String result = buffer.toString();
        LinkedTreeMap treeMap = new Gson().fromJson(result, LinkedTreeMap.class);
        LinkedTreeMap clients = (LinkedTreeMap) treeMap.get("clients");
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals(clients.get("active"), 1.0);
            Assert.assertEquals(clients.get("total"), 2.0);
        });
    }

    @Test
    public void testSubscribeManyTimes() throws Exception {
        MQTT mqttConsumer = createMQTTClient();
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        String topicName1 = "subscribeManyTimes1";
        String topicName2 = "subscribeManyTimes2";
        Topic[] topic1 = { new Topic(topicName1, QoS.AT_LEAST_ONCE)};
        Topic[] topic2 = { new Topic(topicName2, QoS.AT_LEAST_ONCE)};
        consumer.subscribe(topic1);
        consumer.subscribe(topic2);

        MQTT mqttProducer = createMQTTClient();
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        String msg1 = "hello topic1";
        String msg2 = "hello topic2";
        producer.publish(topicName1, msg1.getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        producer.publish(topicName2, msg2.getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        producer.disconnect();
        Message receive1 = consumer.receive();
        Message receive2 = consumer.receive();
        consumer.disconnect();
        Assert.assertEquals(new String(receive1.getPayload()), msg1);
        Assert.assertEquals(receive1.getTopic(), topicName1);
        Assert.assertEquals(new String(receive2.getPayload()), msg2);
        Assert.assertEquals(receive2.getTopic(), topicName2);
    }

    @Test
    public void testLastWillMessage() throws Exception {
        MQTT mqttConsumer = createMQTTClient();
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        String topicName1 = "topic-a";
        String topicName2 = "will-message-topic";
        Topic[] topic1 = { new Topic(topicName1, QoS.AT_LEAST_ONCE)};
        Topic[] topic2 = { new Topic(topicName2, QoS.AT_LEAST_ONCE)};
        consumer.subscribe(topic1);
        consumer.subscribe(topic2);

        MQTT mqttProducer = createMQTTClient();
        mqttProducer.setWillMessage("offline");
        mqttProducer.setWillTopic(topicName2);
        mqttProducer.setWillRetain(false);
        mqttProducer.setWillQos(QoS.AT_LEAST_ONCE);
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        String msg1 = "hello topic1";
        producer.publish(topicName1, msg1.getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        Message receive1 = consumer.receive();
        producer.disconnect();
        Message receive2 = consumer.receive();
        consumer.disconnect();
        Assert.assertEquals(new String(receive1.getPayload()), msg1);
        Assert.assertEquals(receive1.getTopic(), topicName1);
        Assert.assertEquals(new String(receive2.getPayload()), "offline");
        Assert.assertEquals(receive2.getTopic(), "will-message-topic");
    }

    @Test
    public void testTopicUnload() throws Exception {
        MQTT mqttConsumer = createMQTTClient();
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        String topicName1 = "topic-unload-1";
        Topic[] topic1 = { new Topic(topicName1, QoS.AT_LEAST_ONCE)};
        consumer.subscribe(topic1);
        MQTT mqttProducer = createMQTTClient();
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        String msg1 = "hello topic1";
        producer.publish(topicName1, msg1.getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        Message receive1 = consumer.receive();
        Assert.assertEquals(new String(receive1.getPayload()), msg1);
        Assert.assertEquals(receive1.getTopic(), topicName1);
        admin.topics().unload(topicName1);
        Thread.sleep(5000);
        producer.publish(topicName1, msg1.getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        producer.disconnect();
        Message receive2 = consumer.receive();
        Assert.assertEquals(new String(receive2.getPayload()), msg1);
        Assert.assertEquals(receive2.getTopic(), topicName1);
        consumer.disconnect();
    }

    @Test
    public void testNamespaceBundleUnload() throws Exception {
        MQTT mqttConsumer = createMQTTClient();
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        String topicName1 = "topic-namespace-bundle-unload-1";
        Topic[] topic1 = { new Topic(topicName1, QoS.AT_LEAST_ONCE)};
        consumer.subscribe(topic1);
        MQTT mqttProducer = createMQTTClient();
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        String msg1 = "hello msg1";
        producer.publish(topicName1, msg1.getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, false);
        Message receive1 = consumer.receive();
        Assert.assertEquals(new String(receive1.getPayload()), msg1);
        Assert.assertEquals(receive1.getTopic(), topicName1);
        String namespace = "public/default";
        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        bundlesData.getNumBundles();
        for (int i = 0; i < bundlesData.getBoundaries().size() - 1; i++) {
            final String bundle = String.format("%s_%s", bundlesData.getBoundaries().get(i),
                    bundlesData.getBoundaries().get(i + 1));
            admin.namespaces().unloadNamespaceBundle(namespace, bundle);
        }
        Awaitility.await().untilAsserted(() -> Assert.assertTrue(consumer.isConnected()));
        String msg2 = "hello msg2";
        producer.publish(topicName1, msg2.getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, false);
        Message receive2 = consumer.receive();
        Assert.assertEquals(new String(receive2.getPayload()), msg2);
        consumer.disconnect();
        producer.disconnect();
    }


    @Test
    public void testNonPersistentTopic() throws Exception {
        String topicName = "non-persistent://public/default/a/b";
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test
    public void testRetainedMessage() throws Exception {
        String topicName = "persistent://public/default/a";
        MQTT mqtt = createMQTTClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        String message1 = "Retained Message";
        String message2 = "Hello Message";
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.publish(topicName, message1.getBytes(), QoS.AT_LEAST_ONCE, true);
        connection.subscribe(topics);
        connection.publish(topicName, message2.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received1 = connection.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(received1);
        received1.ack();
        Assert.assertEquals(new String(received1.getPayload()), message1);
        Message received2 = connection.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(received2);
        received2.ack();
        Assert.assertEquals(new String(received2.getPayload()), message2);
        connection.disconnect();
    }
}
