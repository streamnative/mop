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

package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.proxy;

import static org.mockito.Mockito.verify;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.TopicFilterImpl;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.psk.PSKClient;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.client.impl.LookupTopicResult;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
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
 * Integration tests for MQTT protocol handler with proxy.
 */
@Slf4j
public class ProxyTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setDefaultNumberOfNamespaceBundles(4);
        mqtt.setMqttProxyEnabled(true);
        mqtt.setMqttProxyTlsPskEnabled(true);
        mqtt.setMqttTlsPskIdentityHint("alpha");
        mqtt.setMqttTlsPskIdentity("mqtt:mqtt123");
        mqtt.getProperties().setProperty("systemEventEnabled", "true");
        return mqtt;
    }

    @Test(timeOut = TIMEOUT)
    public void testBacklogShouldBeZeroWithQos0() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos0";
        MQTT mqtt = createMQTTProxyClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        int numMessages = 20000;
        for (int i = 0; i < numMessages; i++) {
            connection.publish(topicName, (message + i).getBytes(), QoS.AT_MOST_ONCE, false);
        }

        int count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received = connection.receive();
            if (received != null) {
                Assert.assertEquals(message + i, new String(received.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);

        Assert.assertEquals(admin.topics().getStats(topicName).getSubscriptions().size(), 1);
        Assert.assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().entrySet().iterator().next().getValue().getMsgBacklog(), 0);
        connection.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testBacklogShouldBeZeroWithQos1() throws Exception {
        final String topicName = "persistent://public/default/testBacklogShouldBeZeroWithQos1";
        MQTT mqtt = createMQTTProxyClient();
        mqtt.setKeepAlive((short) 5);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        int numMessages = 20000;
        for (int i = 0; i < numMessages; i++) {
            connection.publish(topicName, (message + i).getBytes(), QoS.AT_LEAST_ONCE, false);
        }

        int count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received = connection.receive();
            if (received != null) {
                Assert.assertEquals(message + i, new String(received.getPayload()));
                received.ack();
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);

        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(admin.topics().getStats(topicName).getSubscriptions().size(), 1));
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(admin.topics().getStats(topicName)
                .getSubscriptions().entrySet().iterator().next().getValue().getMsgBacklog(), 0));
        connection.disconnect();
    }

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT, priority = 4)
    public void testSendAndConsume(String topicName) throws Exception {
        MQTT mqtt = createMQTTProxyClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT Proxy";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test(expectedExceptions = {EOFException.class, IllegalStateException.class}, priority = 3)
    public void testInvalidClientId() throws Exception {
        MQTT mqtt = createMQTTProxyClient();
        mqtt.setConnectAttemptsMax(1);
        // ClientId is invalid, for max length is 23 in mqtt 3.1
        mqtt.setClientId(UUID.randomUUID().toString().replace("-", ""));
        BlockingConnection connection = Mockito.spy(mqtt.blockingConnection());
        connection.connect();
        verify(connection, Mockito.times(2)).connect();
    }

    @Test(timeOut = TIMEOUT, priority = 2)
    public void testSendAndConsumeAcrossProxy() throws Exception {
        int numMessage = 3;
        String topicName = "a/b/c";
        MQTT mqtt0 = new MQTT();
        mqtt0.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection0 = mqtt0.blockingConnection();
        connection0.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection0.subscribe(topics);

        String message = "Hello MQTT Proxy";
        MQTT mqtt1 = new MQTT();
        mqtt1.setHost("127.0.0.1", mqttProxyPortList.get(1));
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        connection1.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        MQTT mqtt2 = new MQTT();
        mqtt2.setHost("127.0.0.1", mqttProxyPortList.get(2));
        BlockingConnection connection2 = mqtt2.blockingConnection();
        connection2.connect();
        connection2.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        MQTT mqtt3 = new MQTT();
        mqtt3.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection3 = mqtt3.blockingConnection();
        connection3.connect();
        connection3.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        for (int i = 0; i < numMessage; i++) {
            Message received = connection0.receive();
            Assert.assertEquals(received.getTopic(), topicName);
            Assert.assertEquals(new String(received.getPayload()), message);
            received.ack();
        }

        connection3.disconnect();
        connection2.disconnect();
        connection1.disconnect();
        connection0.disconnect();
    }

    @Test(dataProvider = "mqttTopicNameAndFilter", timeOut = 30000, priority = 1)
    @SneakyThrows
    public void testSendAndConsumeWithFilter(String topic, String filter) {
        MQTT mqtt0 = createMQTTProxyClient();
        BlockingConnection connection0 = mqtt0.blockingConnection();
        connection0.connect();
        Topic[] topics = { new Topic(filter, QoS.AT_MOST_ONCE) };
        String message = "Hello MQTT Proxy";
        MQTT mqtt1 = createMQTTProxyClient();
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        connection1.publish(topic, message.getBytes(), QoS.AT_MOST_ONCE, false);
        // wait for the publish topic has been stored
        Awaitility.await().untilAsserted(() -> {
                    CompletableFuture<List<String>> listOfTopics = pulsarServiceList.get(0).getNamespaceService()
                    .getListOfTopics(NamespaceName.get("public/default"), CommandGetTopicsOfNamespace.Mode.PERSISTENT);
                    Assert.assertTrue(listOfTopics.join().size() >= 1);
        });
        connection0.subscribe(topics);
        connection1.publish(topic, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection0.receive();
        Assert.assertTrue(new TopicFilterImpl(filter).test(received.getTopic()));
        Assert.assertEquals(new String(received.getPayload()), message);

        connection1.disconnect();
        connection0.disconnect();
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
        ChannelFuture channelFuture = client.connect("localhost", mqttProxyPortTlsPskList.get(0))
                .addListener((ChannelFutureListener) future -> {
                    connected.set(future.isSuccess());
                    latch.countDown();
        });
        latch.await();
        Assert.assertTrue(connected.get());
        channelFuture.channel().close();
    }

    @Test
    @SneakyThrows
    public void testProxyProcessPingReq() {
        String topic = "persistent://public/default/a";
        // Producer
        MQTT mqttProducer = createMQTTProxyClient();
        mqttProducer.setKeepAlive((short) 2);
        mqttProducer.setConnectAttemptsMax(0);
        mqttProducer.setReconnectAttemptsMax(0);
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        producer.publish(topic, "Hello MQTT".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        Thread.sleep(4000); // Sleep 2 times of setKeepAlive.
        Assert.assertTrue(producer.isConnected());
        // Check for broker
        CompletableFuture<LookupTopicResult> broker =
                ((PulsarClientImpl) pulsarClient).getLookup().getBroker(TopicName.get(topic));
        AtomicDouble active = new AtomicDouble(0);
        AtomicDouble total = new AtomicDouble(0);
        CompletableFuture<Void> result = new CompletableFuture<>();
        broker.thenAccept(pair -> {
            try {
                HttpClient httpClient = HttpClientBuilder.create().build();
                final String mopEndPoint = "http://localhost:" + (pair.getLogicalAddress().getPort() + 2) + "/mop/stats";
                HttpResponse response = httpClient.execute(new HttpGet(mopEndPoint));
                InputStream inputStream = response.getEntity().getContent();
                InputStreamReader isReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(isReader);
                StringBuffer buffer = new StringBuffer();
                String str;
                while ((str = reader.readLine()) != null){
                    buffer.append(str);
                }
                String ret = buffer.toString();
                LinkedTreeMap treeMap = new Gson().fromJson(ret, LinkedTreeMap.class);
                LinkedTreeMap clients = (LinkedTreeMap) treeMap.get("clients");
                active.set((Double) clients.get("active"));
                result.complete(null);
            } catch (Throwable ex) {
                result.completeExceptionally(ex);
            }
        });
        result.get(1, TimeUnit.MINUTES);
        Assert.assertEquals(active.get(), 1.0);
    }

    @Test
    @SneakyThrows
    public void testPubAndSubWithDifferentTopics() {
        MQTT mqtt = createMQTTProxyClient();
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic("subTopic2", QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);

        MQTT mqtt2 = createMQTTProxyClient();
        BlockingConnection connection2 = mqtt2.blockingConnection();
        connection2.connect();
        Topic[] topics2 = { new Topic("subTopic1", QoS.AT_LEAST_ONCE) };
        connection2.subscribe(topics2);

        connection.publish("subTopic1", "mqtt1".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        connection2.publish("subTopic2", "mqtt2".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);

        Message msg1 = connection2.receive();
        Message msg2 = connection.receive();
        Assert.assertEquals(new String(msg1.getPayload()), "mqtt1");
        Assert.assertEquals(new String(msg2.getPayload()), "mqtt2");
        //
        connection.disconnect();
        connection2.disconnect();
    }

    @Test
    public void testTopicUnload() throws Exception {
        MQTT mqttConsumer = createMQTTProxyClient();
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        String topicName1 = "topic-unload-1";
        Topic[] topic1 = { new Topic(topicName1, QoS.AT_MOST_ONCE)};
        consumer.subscribe(topic1);
        MQTT mqttProducer = createMQTTProxyClient();
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
    public void testClusterId() throws Exception {
        String topicName = "topic-cluster-id";
        int numPartitions = 40;
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        Set<String> ownerBroker = new HashSet<>();
        for (int i = 0; i < numPartitions; i++) {
            String partitionedTopic = topicName + "-partition-" + i;
            String broker = admin.lookups().lookupTopic(partitionedTopic);
            ownerBroker.add(broker);
        }
        Assert.assertTrue(ownerBroker.size() >= 2);
        List<String> brokers = new ArrayList<>(ownerBroker);
        String broker1 = brokers.get(0);
        MQTT mqtt1 = createMQTT(Integer.parseInt(broker1.substring(broker1.lastIndexOf(":") + 1)) + 5);
        mqtt1.setClientId("ab-ab-ab-ab-ab");
        mqtt1.setReconnectAttemptsMax(0);
        BlockingConnection consumer1 = mqtt1.blockingConnection();
        consumer1.connect();

        Thread.sleep(5000);

        String broker2 = brokers.get(1);
        MQTT mqtt2 = createMQTT(Integer.parseInt(broker2.substring(broker2.lastIndexOf(":") + 1)) + 5);
        mqtt2.setClientId("ab-ab-ab-ab-ab");
        mqtt2.setReconnectAttemptsMax(0);
        BlockingConnection consumer2 = mqtt2.blockingConnection();
        // Due to ip restrict, comment this.
//        Awaitility.await().untilAsserted(() -> Assert.assertFalse(consumer1.isConnected()));
        consumer2.connect();
        consumer1.disconnect();
        consumer2.disconnect();
    }

    @Test
    public void testLastWillMessageInCluster() throws Exception {
        String topicName = "topic-last-will-message";
        int numPartitions = 40;
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        Set<String> ownerBroker = new HashSet<>();
        for (int i = 0; i < numPartitions; i++) {
            String partitionedTopic = topicName + "-partition-" + i;
            String broker = admin.lookups().lookupTopic(partitionedTopic);
            ownerBroker.add(broker);
        }
        Assert.assertTrue(ownerBroker.size() >= 2);
        List<String> brokers = new ArrayList<>(ownerBroker);
        String broker1 = brokers.get(0);
        //
        String willTopic = "will-message-topic";
        String willMessage = "offline";
        MQTT mqtt1 = createMQTT(Integer.parseInt(broker1.substring(broker1.lastIndexOf(":") + 1)) + 5);
        mqtt1.setWillMessage("offline");
        mqtt1.setWillTopic(willTopic);
        mqtt1.setWillRetain(false);
        mqtt1.setWillQos(QoS.AT_LEAST_ONCE);
        mqtt1.setClientId("ab-ab-ab-last-will");
        BlockingConnection producer = mqtt1.blockingConnection();
        producer.connect();
        String msg1 = "any-msg";
        producer.publish("any-topic", msg1.getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, false);
        //
        String broker2 = brokers.get(1);
        MQTT mqtt2 = createMQTT(Integer.parseInt(broker2.substring(broker2.lastIndexOf(":") + 1)) + 5);
        mqtt2.setClientId("cd-cd-cd-last-will");
        BlockingConnection consumer2 = mqtt2.blockingConnection();
        consumer2.connect();
        Topic[] topic = { new Topic(willTopic, QoS.AT_LEAST_ONCE)};
        admin.topics().createNonPartitionedTopic(willTopic);
        consumer2.subscribe(topic);
        producer.disconnect();
        // Due to ip restrict, comment this.
//        Message rev2 = consumer2.receive(30, TimeUnit.SECONDS);
//        Assert.assertNotNull(rev2);
//        Assert.assertEquals(new String(rev2.getPayload()), willMessage);
//        Assert.assertEquals(rev2.getTopic(), willTopic);
        consumer2.disconnect();
    }

    @Test
    public void testRetainedMessageInCluster() throws Exception {
        String topicName = "topic-retained-message";
        int numPartitions = 40;
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        Set<String> ownerBroker = new HashSet<>();
        for (int i = 0; i < numPartitions; i++) {
            String partitionedTopic = topicName + "-partition-" + i;
            String broker = admin.lookups().lookupTopic(partitionedTopic);
            ownerBroker.add(broker);
        }
        Assert.assertTrue(ownerBroker.size() >= 2);
        List<String> brokers = new ArrayList<>(ownerBroker);
        String broker1 = brokers.get(0);
        //
        String retainedTopic = "retained-message-topic";
        String retainedMessage = "retained message";
        MQTT mqtt1 = createMQTT(Integer.parseInt(broker1.substring(broker1.lastIndexOf(":") + 1)) + 5);
        mqtt1.setClientId("ab-ab-ab");
        BlockingConnection producer = mqtt1.blockingConnection();
        producer.connect();
        producer.publish(retainedTopic, retainedMessage.getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, true);
        producer.disconnect();
        // Wait for the retained message to be sent out and reader received.
        Thread.sleep(5000);
        //
        String broker2 = brokers.get(1);
        MQTT mqtt2 = createMQTT(Integer.parseInt(broker2.substring(broker2.lastIndexOf(":") + 1)) + 5);
        mqtt2.setClientId("cd-cd-cd");
        BlockingConnection consumer2 = mqtt2.blockingConnection();
        consumer2.connect();
        Topic[] topic = { new Topic(retainedTopic, QoS.AT_LEAST_ONCE)};
        consumer2.subscribe(topic);
        Message rev2 = consumer2.receive();
        Assert.assertNotNull(rev2);
        Assert.assertEquals(new String(rev2.getPayload()), retainedMessage);
        consumer2.disconnect();
    }

    @Test
    @SneakyThrows
    public void testAddPskIdentity() {
        HttpClient httpClient = HttpClientBuilder.create().build();
        final String mopEndPoint = "http://localhost:" + brokerWebservicePortList.get(0) + "/mop/add_psk_identity";
        HttpPost request = new HttpPost();
        request.setURI(new URI(mopEndPoint));
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair("identity", "mqtt2:mqtt222;mqtt3:mqtt333"));
        request.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
        HttpResponse response = httpClient.execute(request);
        InputStream inputStream = response.getEntity().getContent();
        InputStreamReader isReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer buffer = new StringBuffer();
        String str;
        while ((str = reader.readLine()) != null){
            buffer.append(str);
        }
        Assert.assertTrue(buffer.toString().equals("OK"));
        Thread.sleep(3000);
        Bootstrap client = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        client.group(group);
        client.channel(NioSocketChannel.class);
        client.handler(new PSKClient("alpha", "mqtt2", "mqtt222"));
        AtomicBoolean connected = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        ChannelFuture cf = client.connect("localhost", mqttProxyPortTlsPskList.get(0))
                .addListener((ChannelFutureListener) future -> {
                    connected.set(future.isSuccess());
                    latch.countDown();
        });
        latch.await();
        Assert.assertTrue(connected.get());
        cf.channel().close();
    }

    @Test(timeOut = TIMEOUT)
    public void testSendConsumeFromDifferentProxy() throws Exception {
        final String topicName = "persistent://public/default/testSendConsumeFromDifferentProxy";
        MQTT mqtt1 = new MQTT();
        mqtt1.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection1.subscribe(topics);

        MQTT mqtt2 = new MQTT();
        mqtt2.setHost("127.0.0.1", mqttProxyPortList.get(1));
        BlockingConnection connection2 = mqtt2.blockingConnection();
        connection2.connect();
        connection2.subscribe(topics);

        MQTT mqtt3 = new MQTT();
        mqtt3.setHost("127.0.0.1", mqttProxyPortList.get(2));
        BlockingConnection connection3 = mqtt3.blockingConnection();
        connection3.connect();
        connection3.subscribe(topics);

        String message = "Hello MQTT";
        int numMessages = 1000;

        for (int i = 0; i < numMessages; i++) {
            connection1.publish(topicName, (message + i).getBytes(), QoS.AT_MOST_ONCE, false);
        }

        int count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received1 = connection1.receive();
            if (received1 != null) {
                Assert.assertEquals(message + i, new String(received1.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received2 = connection2.receive();
            if (received2 != null) {
                Assert.assertEquals(message + i, new String(received2.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received3 = connection3.receive();
            if (received3 != null) {
                Assert.assertEquals(message + i, new String(received3.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        connection3.disconnect();
        connection2.disconnect();
        connection1.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testSendConsumeWithSameProxy() throws Exception {
        final String topicName = "persistent://public/default/testSendConsumeWithSameProxy";
        MQTT mqtt1 = new MQTT();
        mqtt1.setClientId("client-1");
        mqtt1.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection1 = mqtt1.blockingConnection();
        connection1.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection1.subscribe(topics);

        MQTT mqtt2 = new MQTT();
        mqtt2.setClientId("client-2");
        mqtt2.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection2 = mqtt2.blockingConnection();
        connection2.connect();
        connection2.subscribe(topics);

        MQTT mqtt3 = new MQTT();
        mqtt3.setClientId("client-3");
        mqtt3.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection3 = mqtt3.blockingConnection();
        connection3.connect();
        connection3.subscribe(topics);

        MQTT mqtt4 = new MQTT();
        mqtt4.setClientId("client-4");
        mqtt4.setHost("127.0.0.1", mqttProxyPortList.get(0));
        BlockingConnection connection4 = mqtt4.blockingConnection();
        connection4.connect();
        connection4.subscribe(topics);

        String message = "Hello MQTT";
        int numMessages = 1000;

        for (int i = 0; i < numMessages; i++) {
            connection1.publish(topicName, (message + i).getBytes(), QoS.AT_MOST_ONCE, false);
        }

        int count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received1 = connection1.receive();
            if (received1 != null) {
                Assert.assertEquals(message + i, new String(received1.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received2 = connection2.receive();
            if (received2 != null) {
                Assert.assertEquals(message + i, new String(received2.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received3 = connection3.receive();
            if (received3 != null) {
                Assert.assertEquals(message + i, new String(received3.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        count = 0;
        for (int i = 0; i < numMessages; i++) {
            Message received4 = connection4.receive();
            if (received4 != null) {
                Assert.assertEquals(message + i, new String(received4.getPayload()));
                count++;
            }
        }
        Assert.assertEquals(count, numMessages);
        connection4.disconnect();
        connection3.disconnect();
        connection2.disconnect();
        connection1.disconnect();
    }
}
