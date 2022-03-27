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
package io.streamnative.pulsar.handlers.mqtt.base;

import io.streamnative.pulsar.handlers.mqtt.MQTTProtocolHandler;
import io.streamnative.pulsar.handlers.mqtt.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.event.PulsarTopicChangeListener;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EventCenterTest extends MQTTTestBase {

    @Test
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
