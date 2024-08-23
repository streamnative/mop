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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.jsonwebtoken.SignatureAlgorithm;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.io.File;
import java.io.FileInputStream;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.util.SecurityUtility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;


import static org.mockito.Mockito.spy;

public class ProxyMtlsTest extends MQTTTestBase {


    String path = "./src/test/resources/mtls/";

    String token;

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        System.setProperty("jdk.security.allowNonCaAnchor", "true");
        enableTls = true;
        MQTTCommonConfiguration mqtt = super.initConfig();

        mqtt.setSystemTopicEnabled(false);
        mqtt.setMqttProxyEnabled(true);
        mqtt.setMqttProxyMtlsEnabled(true);
        mqtt.setMqttMtlsEnabled(true);
        mqtt.setMqttProxyTlsEnabled(true);

        mqtt.setMqttTlsEnabledWithKeyStore(true);
        mqtt.setMqttTlsKeyStoreType("JKS");
        mqtt.setMqttTlsKeyStore(path + "serverkeystore.jks");
        mqtt.setMqttTlsKeyStorePassword("123456");
        mqtt.setMqttTlsTrustStoreType("JKS");
        mqtt.setMqttTlsTrustStore(path + "truststore.jks");
        mqtt.setMqttTlsTrustStorePassword("123456");

        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        token = AuthTokenUtils.createToken(secretKey, "superUser", Optional.empty());

        mqtt.setAuthenticationEnabled(true);
        mqtt.setMqttAuthenticationEnabled(true);
        mqtt.setMqttAuthenticationMethods(ImmutableList.of("token"));
        mqtt.setSuperUserRoles(ImmutableSet.of("superUser"));
        mqtt.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        mqtt.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        mqtt.setBrokerClientAuthenticationParameters("token:" + token);
        mqtt.setProperties(properties);

        return mqtt;
    }

    @Override
    public void afterSetup() throws Exception {
        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure("token:" + token);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .authentication(authToken)
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        admin = spy(PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(authToken)
                .build());
    }

    public SSLContext createSSLContext() throws Exception {
        File caCertFile = new File(path + "ca.cer");
        Certificate caCert = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(caCertFile));

        File clientCertFile = new File(path + "client.crt");
        Certificate clientCert = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(clientCertFile));

        PrivateKey privateKey = SecurityUtility.loadPrivateKeyFromPemFile(path + "client.key");

        final SSLContext sslContext = SecurityUtility.createSslContext(true,
                new Certificate[]{caCert}, new Certificate[]{clientCert}, privateKey);

        return sslContext;
    }

    public SSLContext createSSLContext2() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(new FileInputStream(path + "client.p12"), "".toCharArray());

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(new FileInputStream(path + "ca.p12"), "".toCharArray());

        // 初始化密钥管理器
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, "".toCharArray());

        // 初始化信任管理器
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        return sslContext;
    }


    @Test
    public void testMqtt3() throws Exception {


        SSLContext sslContext = createSSLContext2();
        MQTT mqtt = createMQTTProxyTlsClient();
        mqtt.setSslContext(sslContext);

        String topicName = "testProduceAndConsume";
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
//        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
//        Message received = connection.receive();
//        Assert.assertEquals(received.getTopic(), topicName);
//        Assert.assertEquals(new String(received.getPayload()), message);
//        received.ack();
        connection.disconnect();

    }

    @Test
    public void testMqtt5() throws Exception {

        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(path + "clientkeystore.jks"), "123456".toCharArray());

        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(new FileInputStream(path + "truststore.jks"), "123456".toCharArray());

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, "123456".toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        final MqttClientSslConfig sslConfig = MqttClientSslConfig.builder().keyManagerFactory(keyManagerFactory).trustManagerFactory(trustManagerFactory)
                .build();

        Random random = new Random();
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .sslConfig(sslConfig)
                .serverPort(getMqttProxyPortTlsList().get(random.nextInt(getMqttProxyPortTlsList().size())))
                .buildBlocking();

        String topic = "testMqtt5";
        client.connect();
        client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
        byte[] msg = "payload".getBytes();
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg)
                .send();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5Publish publish = publishes.receive();
            Assert.assertEquals(publish.getTopic(), MqttTopic.of(topic));
            Assert.assertEquals(publish.getPayloadAsBytes(), msg);
        }
        client.unsubscribeWith().topicFilter(topic).send();
        client.disconnect();
    }
}
