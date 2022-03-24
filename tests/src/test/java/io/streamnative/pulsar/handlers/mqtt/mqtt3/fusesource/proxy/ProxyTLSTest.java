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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.TopicFilterImpl;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.psk.PSKClient;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class ProxyTLSTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();

        mqtt.setMqttProxyEnabled(true);
        mqtt.setMqttProxyTlsEnabled(true);
        mqtt.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        mqtt.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        mqtt.setMqttProxyTlsPskEnabled(true);
        mqtt.setTlsPskIdentityHint("alpha");
        mqtt.setTlsPskIdentity("mqtt:mqtt123");

        return mqtt;
    }

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT, priority = 3)
    public void testSendAndConsumeUsingTLS(String topicName) throws Exception {
        MQTT mqtt = createMQTTProxyTlsClient();
        File crtFile = new File(TLS_SERVER_CERT_FILE_PATH);
        Certificate certificate = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(crtFile));
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("server", certificate);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        mqtt.setSslContext(sslContext);
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

    @Test(dataProvider = "mqttTopicNames", timeOut = TIMEOUT, priority = 2)
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

    @Test(dataProvider = "mqttTopicNameAndFilter", timeOut = 20000, priority = 1)
    @SneakyThrows
    public void testSendAndConsumeWithFilter(String topic, String filter) {
        MQTT mqtt0 = createMQTTProxyTlsClient();
        // Set cert and keystore
        File crtFile = new File(TLS_SERVER_CERT_FILE_PATH);
        Certificate certificate = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(crtFile));
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("server", certificate);
        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        mqtt0.setSslContext(sslContext);
        //
        BlockingConnection connection0 = mqtt0.blockingConnection();
        connection0.connect();
        Topic[] topics = { new Topic(filter, QoS.AT_MOST_ONCE) };
        String message = "Hello MQTT Proxy";
        MQTT mqtt1 = createMQTTProxyTlsClient();
        mqtt1.setSslContext(sslContext);
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

    @Test(timeOut = TIMEOUT, priority = 4)
    @SneakyThrows
    public void testTlsPsk() {
        Bootstrap client = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        client.group(group);
        client.channel(NioSocketChannel.class);
        client.handler(new PSKClient("alpha", "mqtt", "mqtt123"));
        AtomicBoolean connected = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        client.connect("localhost", mqttProxyPortTlsPskList.get(0)).addListener((ChannelFutureListener) future -> {
            connected.set(future.isSuccess());
            latch.countDown();
        });
        latch.await();
        Assert.assertTrue(connected.get());
    }
}
