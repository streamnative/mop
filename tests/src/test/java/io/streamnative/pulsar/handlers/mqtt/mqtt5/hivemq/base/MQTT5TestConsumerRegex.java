package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;


public class MQTT5TestConsumerRegex extends MQTTTestBase {

    @Test(timeOut = TIMEOUT)
    public void testConsumerRegex() throws Exception {
        String firstTopic = "/test/1/1";
        String secondTopic = "/test/1/2";
        final String topicFilter = "/test/#";
        Mqtt5BlockingClient client = MQTT5ClientUtils.createMqtt5Client(getMqttBrokerPortList().get(0));
        client.connectWith()
                .cleanStart(true)
                .send();
        client.subscribeWith()
                .topicFilter(topicFilter)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes consumer = client.publishes(MqttGlobalPublishFilter.ALL);
        client.publishWith()
                .topic(firstTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("test1".getBytes(StandardCharsets.UTF_8))
                .send();
        client.publishWith()
                .topic(secondTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("test2".getBytes(StandardCharsets.UTF_8))
                .send();
        Awaitility.await()
                .untilAsserted(()-> {
                    List<String> topics = admin.namespaces().getTopics("public/default");
                    Assert.assertEquals(topics.size(), 2);
                });
        Awaitility.await()
                .untilAsserted(()-> {
                    List<String> topics = admin.namespaces().getTopics("public/default");
                    List<String> subscriptions = admin.topics().getSubscriptions(topics.get(0));
                    Assert.assertEquals(subscriptions.size(), 1);
                    List<String> subscriptions2 = admin.topics().getSubscriptions(topics.get(1));
                    Assert.assertEquals(subscriptions2.size(), 1);
                });
        client.publishWith()
                .topic(firstTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("test3".getBytes(StandardCharsets.UTF_8))
                .send();
        client.publishWith()
                .topic(secondTopic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload("test4".getBytes(StandardCharsets.UTF_8))
                .send();
        Mqtt5Publish msg1 = consumer.receive();
        Mqtt5Publish msg2 = consumer.receive();
        Mqtt5Publish msg3 = consumer.receive();
        Mqtt5Publish msg4 = consumer.receive();
        String msg1Topic = msg1.getTopic().toString();
        String msg2Topic = msg2.getTopic().toString();
        String msg3Topic = msg3.getTopic().toString();
        String msg4Topic = msg4.getTopic().toString();
        String msgPayload1 = new String(msg1.getPayloadAsBytes());
        String msgPayload2 = new String(msg2.getPayloadAsBytes());
        String msgPayload3 = new String(msg3.getPayloadAsBytes());
        String msgPayload4 = new String(msg4.getPayloadAsBytes());
        Assert.assertTrue(Objects.equals(msg1Topic, firstTopic) || Objects.equals(msg1Topic, secondTopic));
        Assert.assertTrue(Objects.equals(msgPayload1, "test1") || Objects.equals(msgPayload1, "test2"));
        Assert.assertTrue(Objects.equals(msg2Topic, firstTopic) || Objects.equals(msg2Topic, secondTopic));
        Assert.assertTrue(Objects.equals(msgPayload2, "test1") || Objects.equals(msgPayload2, "test2"));
        Assert.assertTrue(Objects.equals(msg3Topic, firstTopic) || Objects.equals(msg3Topic, secondTopic));
        Assert.assertTrue(Objects.equals(msgPayload3, "test3") || Objects.equals(msgPayload3, "test4"));
        Assert.assertTrue(Objects.equals(msg4Topic, firstTopic) || Objects.equals(msg4Topic, secondTopic));
        Assert.assertTrue(Objects.equals(msgPayload4, "test3") || Objects.equals(msgPayload4, "test4"));
    }
}
