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

import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.apache.pulsar.common.util.SecurityUtility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProxyMtlsTest extends MQTTTestBase {

    String path = "./src/test/resources/mtls/";

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        System.setProperty("javax.net.debug","ssl");
        enableTls = true;
        MQTTCommonConfiguration mqtt = super.initConfig();

//        mqtt.setMqttProxyEnabled(true);
//        mqtt.setMqttProxyTlsEnabled(true);
        mqtt.setMqttTlsCertificateFilePath(path + "server.crt");
        mqtt.setMqttTlsTrustCertsFilePath(path + "client_combined.pem");
        mqtt.setMqttTlsKeyFilePath(path + "server.key");

        return mqtt;
    }

    public SSLContext createSSLContext() throws Exception {
//        KeyStore keyStore = KeyStore.getInstance("PKCS12");
//        try (FileInputStream fis = new FileInputStream(path + "client.p12")) {
//            keyStore.load(fis, "".toCharArray());
//
//        }



        File clientCrt = new File(path + "client.crt");
        Certificate clientCert = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(clientCrt));
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("client", clientCert);

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

    public SSLContext createSSLContext2() throws Exception {
        File clientCrt1 = new File(path + "ca.cer");
        Certificate clientCert1 = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(clientCrt1));


        File key = new File(path + "client.crt");
        Certificate clientKey1 = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(key));

        PrivateKey privateKey = SecurityUtility.loadPrivateKeyFromPemFile(path + "client.crt.pem");


        final SSLContext sslContext = SecurityUtility.createSslContext(true, new Certificate[]{clientCert1}, new Certificate[]{clientKey1}, privateKey);

        return sslContext;
    }


    @Test
    public void testProduceAndConsume() throws Exception {
        SSLContext sslContext = createSSLContext2();
        MQTT mqtt = createMQTTTlsClient();
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

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
}
