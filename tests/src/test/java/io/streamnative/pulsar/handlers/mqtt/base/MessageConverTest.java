package io.streamnative.pulsar.handlers.mqtt.base;

import com.google.api.client.util.Lists;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class MessageConverTest extends MQTTTestBase{

    MQTTServerConfiguration serverConfiguration;

    String mqttTopic = "test";

    String mqttEventTimeFromProp = "eventtime";

    PulsarService pulsarService;

    Topic topic;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        super.setup();
        serverConfiguration = new MQTTServerConfiguration();
        serverConfiguration.setMqttAutoEventTime(true);

        List<PulsarService> pulsarServiceList = getPulsarServiceList();
        pulsarService = pulsarServiceList.get(0);

        String encodedPulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(mqttTopic, serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), TopicDomain.persistent);
        pulsarService.getAdminClient().topics().createNonPartitionedTopic(encodedPulsarTopicName);

        CompletableFuture<Optional<Topic>> topicReference = PulsarTopicUtils.getTopicReference(pulsarService, mqttTopic,
                serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), true
                , serverConfiguration.getDefaultTopicDomain());

        topic = topicReference.get().get();
    }

    @Test
    public void testAutoEventTime(){
        MessageImpl<byte[]> message = PulsarMessageConverter.toPulsarMsg(serverConfiguration, topic, new MqttProperties(), ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(true,message.getEventTime()>0);
        serverConfiguration.setMqttAutoEventTime(false);
        MessageImpl<byte[]> notEventTimeMessage = PulsarMessageConverter.toPulsarMsg(serverConfiguration, topic, new MqttProperties(), ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(true,notEventTimeMessage.getEventTime()==0);
    }

    @Test
    public void testFillEventTimeFromProp(){
        serverConfiguration.setMqttEventTimeFromProp(mqttEventTimeFromProp);

        List<MqttProperties.StringPair> list = Lists.newArrayList();
        long eventtime = System.currentTimeMillis();
        list.add(new MqttProperties.StringPair(mqttEventTimeFromProp,eventtime+""));

        MqttProperties mp = new MqttProperties();
        mp.add(new MqttProperties.UserProperties(list));

        MessageImpl<byte[]> message = PulsarMessageConverter.toPulsarMsg(serverConfiguration, topic, mp, ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(eventtime,message.getEventTime());
    }

}
