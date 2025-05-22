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

import static io.streamnative.oidc.broker.common.pojo.Pool.AUTH_TYPE_MTLS;
import static org.mockito.Mockito.spy;
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
import io.streamnative.oidc.broker.common.OIDCPoolResources;
import io.streamnative.oidc.broker.common.pojo.Pool;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.util.SecurityUtility;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Ignore
public class ProxyMtlsTest extends MQTTTestBase {


    String path = "./src/test/resources/mtls/proxy/";

    String token;

    MQTTCommonConfiguration localConfig;

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        System.setProperty("jdk.security.allowNonCaAnchor", "true");
        enableTls = true;
        MQTTCommonConfiguration mqtt = super.initConfig();

        mqtt.setSystemTopicEnabled(false);
        mqtt.setMqttProxyEnabled(true);
        mqtt.setMqttProxyMTlsAuthenticationEnabled(true);
        mqtt.setMqttProxyTlsEnabled(true);
        mqtt.setMqttTlsRequireTrustedClientCertOnConnect(true);
        mqtt.setMqttTlsAllowInsecureConnection(false);

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

        localConfig = mqtt;

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

        final PulsarService pulsarService = pulsarServiceList.get(0);
        OIDCPoolResources oidcPoolResources = new OIDCPoolResources(pulsarService.getLocalMetadataStore());

        Pool pool = new Pool("test-pool", AUTH_TYPE_MTLS, "d", "provider-1", "CN=='CLIENT'");
        oidcPoolResources.createPool(pool);

        Awaitility.await().until(() -> oidcPoolResources.getPool("test-pool") != null);
    }

    public SSLContext createSSLContext() throws Exception {
        File caCertFile = new File(path + "ca.cer");
        Certificate caCert = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(caCertFile));

        File clientCertFile = new File(path + "client.cer");
        Certificate clientCert = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(clientCertFile));

        PrivateKey privateKey = SecurityUtility.loadPrivateKeyFromPemFile(path + "client.key");

        final SSLContext sslContext = SecurityUtility.createSslContext(true,
                new Certificate[]{caCert}, new Certificate[]{clientCert}, privateKey);

        return sslContext;
    }

    @Test
    public void testMqtt3() throws Exception {
        SSLContext sslContext = createSSLContext();
        MQTT mqtt = createMQTTProxyTlsClient();
        mqtt.setSslContext(sslContext);

        String topicName = "mqtt3";
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT3";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
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

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        final MqttClientSslConfig sslConfig = MqttClientSslConfig.builder().keyManagerFactory(keyManagerFactory)
                .trustManagerFactory(trustManagerFactory)
                .build();

        Random random = new Random();
        final Mqtt5BlockingClient client1 = Mqtt5Client.builder()
                .identifier("client-1")
                .serverHost("localhost")
                .sslConfig(sslConfig)
                .serverPort(getMqttProxyPortTlsList().get(random.nextInt(getMqttProxyPortTlsList().size())))
                .buildBlocking();
        final Mqtt5BlockingClient client2 = Mqtt5Client.builder()
                .identifier("client-2")
                .serverHost("localhost")
                .sslConfig(sslConfig)
                .serverPort(getMqttProxyPortTlsList().get(random.nextInt(getMqttProxyPortTlsList().size())))
                .buildBlocking();

        String topic1 = "testMqtt5-client-1";
        String topic2 = "testMqtt5-client-2";

        client1.connect();
        client2.connect();
        client1.subscribeWith().topicFilter(topic1).qos(MqttQos.AT_LEAST_ONCE).send();
        client2.subscribeWith().topicFilter(topic2).qos(MqttQos.AT_LEAST_ONCE).send();
        byte[] msg1 = "client-1-payload".getBytes();
        byte[] msg2 = "client-2-payload".getBytes();
        client1.publishWith()
                .topic(topic1)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(msg1)
                .send();

        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client1.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5Publish publish = publishes.receive();
            Assert.assertEquals(publish.getTopic(), MqttTopic.of(topic1));
            Assert.assertEquals(publish.getPayloadAsBytes(), msg1);
        }
        //
        client2.publishWith()
            .topic(topic2)
            .qos(MqttQos.AT_LEAST_ONCE)
            .payload(msg2)
            .send();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL)) {
            Mqtt5Publish publish = publishes.receive();
            Assert.assertEquals(publish.getTopic(), MqttTopic.of(topic2));
            Assert.assertEquals(publish.getPayloadAsBytes(), msg2);
        }
        client1.unsubscribeWith().topicFilter(topic1).send();
        client1.disconnect();
        client2.unsubscribeWith().topicFilter(topic1).send();
        client2.disconnect();
    }
}
