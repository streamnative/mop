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

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.base.PortManager;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class TLSTest extends MQTTTestBase {

    @Override
    protected void resetConfig() {
        MQTTServerConfiguration mqtt = new MQTTServerConfiguration();
        mqtt.setAdvertisedAddress("localhost");
        mqtt.setClusterName(configClusterName);

        mqtt.setManagedLedgerCacheSizeMB(8);
        mqtt.setActiveConsumerFailoverDelayTimeMillis(0);
        mqtt.setDefaultRetentionTimeInMinutes(7);
        mqtt.setDefaultNumberOfNamespaceBundles(1);
        mqtt.setZookeeperServers("localhost:2181");
        mqtt.setConfigurationStoreServers("localhost:3181");

        mqtt.setTlsEnabled(true);
        mqtt.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        mqtt.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);

        mqtt.setAuthenticationEnabled(false);
        mqtt.setAuthorizationEnabled(false);
        mqtt.setAllowAutoTopicCreation(true);
        mqtt.setBrokerDeleteInactiveTopicsEnabled(false);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("test-protocol-handler.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            return;
        }

        String protocolHandlerDir = handlerPath.toFile().getParent();

        mqtt.setProtocolHandlerDirectory(
                protocolHandlerDir
        );
        mqtt.setMessagingProtocols(Sets.newHashSet("mqtt"));

        this.conf = mqtt;
    }

    @Override
    protected void startBroker() throws Exception {
        for (int i = 0; i < brokerCount; i++) {

            int brokerPort = PortManager.nextFreePort();
            brokerPortList.add(brokerPort);
            int brokerPortTls = PortManager.nextFreePort();
            brokerPortList.add(brokerPortTls);

            int mqttBrokerPort = PortManager.nextFreePort();
            mqttBrokerPortList.add(mqttBrokerPort);
            int mqttBrokerTlsPort = PortManager.nextFreePort();
            mqttBrokerPortList.add(mqttBrokerTlsPort);

            int mqttProxyPort = PortManager.nextFreePort();
            mqttProxyPortList.add(mqttProxyPort);

            int brokerWebServicePort = PortManager.nextFreePort();
            brokerWebservicePortList.add(brokerWebServicePort);

            int brokerWebServicePortTls = PortManager.nextFreePort();
            brokerWebServicePortTlsList.add(brokerWebServicePortTls);

            conf.setBrokerServicePort(Optional.of(brokerPort));
            String plaintextListener = "mqtt://127.0.0.1:" + mqttBrokerPort;
            String tlsListener = "mqtt+ssl://127.0.0.1:" + mqttBrokerTlsPort;
            ((MQTTServerConfiguration) conf).setMqttListeners(Joiner.on(",").join(plaintextListener, tlsListener));
            ((MQTTServerConfiguration) conf).setMqttProxyPort(mqttProxyPort);
            ((MQTTServerConfiguration) conf).setMqttProxyEnable(true);
            conf.setBrokerServicePortTls(Optional.of(brokerPortTls));
            conf.setWebServicePort(Optional.of(brokerWebServicePort));
            conf.setWebServicePortTls(Optional.of(brokerWebServicePortTls));

            log.info("Start broker info [{}], brokerPort: {}, mqttBrokerPort: {}, mqttProxyPort: {}",
                    i, brokerPort, mqttBrokerPort, mqttProxyPort);
            this.pulsarServiceList.add(startBroker(conf));
        }
    }

    @Test(dataProvider = "mqttTopicNames")
    public void testSimpleMqttPubAndSubQos0Tls(String topicName) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setHost(URI.create("ssl://127.0.0.1:" + getMqttBrokerPortList().get(1)));

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
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }
}
