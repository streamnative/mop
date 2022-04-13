package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.proxy;

import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.fusesource.mqtt.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProxyToPulsarTlsTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setMqttProxyEnabled(true);
        mqtt.setTlsEnabled(true);
        mqtt.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        mqtt.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        return mqtt;
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

}
