package io.streamnative.pulsar.handlers.mqtt.base;

import io.streamnative.pulsar.handlers.mqtt.MQTTProtocolHandler;
import io.streamnative.pulsar.handlers.mqtt.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.event.PulsarTopicChangeListener;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Test
public class EventCenterTest extends MQTTTestBase {

    public void testReceiveNotification() throws PulsarAdminException, InterruptedException, ExecutionException {
        String topicName = "persistent://public/default/testEventCenter";
        List<PulsarService> pulsarServiceList = getPulsarServiceList();
        PulsarService pulsarService = pulsarServiceList.get(0);
        ProtocolHandler mqtt = pulsarService.getProtocolHandlers().protocol("mqtt");
        MQTTProtocolHandler protocolHandler = (MQTTProtocolHandler) mqtt;
        PulsarEventCenter eventCenter = protocolHandler.getMqttService().getEventCenter();
        CompletableFuture<String> onLoadEvent = new CompletableFuture<>();
        CompletableFuture<String> unLoadEvent = new CompletableFuture<>();
        eventCenter.register(new PulsarTopicChangeListener() {

            @Override
            public void onTopicLoad(TopicName topicName) {
                onLoadEvent.complete(topicName.toString());
            }

            @Override
            public void onTopicUnload(TopicName topicName) {
                unLoadEvent.complete(topicName.toString());
            }
        });

        admin.topics().createNonPartitionedTopic(topicName);
        Assert.assertEquals(onLoadEvent.get(), topicName);
        admin.topics().delete(topicName);
        Assert.assertEquals(unLoadEvent.get(), topicName);
    }


}
