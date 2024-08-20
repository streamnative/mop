package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.proxy;

import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Random;

public class ProxyMtlsTest extends MQTTTestBase {

    String path = "./src/test/resources/mtls/";

    private final Random random = new Random();

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        enableTls = true;
        MQTTCommonConfiguration mqtt = super.initConfig();

        mqtt.setMqttProxyEnabled(true);
        mqtt.setMqttProxyTlsEnabled(true);
        mqtt.setMqttTlsCertificateFilePath(path + "server.crt");
        mqtt.setMqttTlsKeyFilePath(path + "server.key");

        return mqtt;
    }

    public SSLContext createSSLContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(path + "client.p12")) {
            keyStore.load(fis, "".toCharArray());
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "".toCharArray());

        File crtFile = new File(path + "server.crt");
        Certificate certificate = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(crtFile));
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        trustStore.setCertificateEntry("server", certificate);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new java.security.SecureRandom());

        return sslContext;
    }

    @Test
    public void testProduceAndConsume() throws Exception {
        SSLContext sslContext = createSSLContext();
        MQTT mqtt = createMQTTProxyTlsClient();
        mqtt.setSslContext(sslContext);

        String topicName = "testProduceAndConsume";
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
}
