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
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import javax.net.ssl.SSLContext;
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
        enableTls = true;
        MQTTCommonConfiguration mqtt = super.initConfig();

        mqtt.setMqttProxyEnabled(true);
        mqtt.setMqttProxyTlsEnabled(true);
        mqtt.setMqttTlsCertificateFilePath(path + "server.crt");
        mqtt.setMqttTlsKeyFilePath(path + "server.key");

        return mqtt;
    }

    public SSLContext createSSLContext() throws Exception {
        File clientCrt1 = new File(path + "ca.cer");
        Certificate clientCert1 = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(clientCrt1));

        File key = new File(path + "client.crt");
        Certificate clientKey1 = CertificateFactory
                .getInstance("X.509").generateCertificate(new FileInputStream(key));

        PrivateKey privateKey = SecurityUtility.loadPrivateKeyFromPemFile(path + "client.key");

        final SSLContext sslContext = SecurityUtility.createSslContext(true,
                new Certificate[]{clientCert1}, new Certificate[]{clientKey1}, privateKey);

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
