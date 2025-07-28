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

import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.broker.impl.PulsarMessageConverter;
import io.streamnative.pulsar.handlers.mqtt.common.utils.PulsarTopicUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;



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

        String encodedPulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(
                mqttTopic, serverConfiguration.getDefaultTenant(),
                serverConfiguration.getDefaultNamespace(), TopicDomain.persistent);
        pulsarService.getAdminClient().topics().createNonPartitionedTopic(encodedPulsarTopicName);

        CompletableFuture<Optional<Topic>> topicReference = PulsarTopicUtils.getTopicReference(pulsarService, mqttTopic,
                serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), true
                , serverConfiguration.getDefaultTopicDomain());

        topic = topicReference.get().get();
    }

    @Test
    public void testAutoEventTime(){
        MessageImpl<byte[]> message = PulsarMessageConverter.toPulsarMsg(
                serverConfiguration, topic, new MqttProperties(), ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(true, message.getEventTime() > 0);
        serverConfiguration.setMqttAutoEventTime(false);
        MessageImpl<byte[]> notEventTimeMessage = PulsarMessageConverter.toPulsarMsg(
                serverConfiguration, topic, new MqttProperties(), ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(true, notEventTimeMessage.getEventTime() == 0);
    }

    @Test
    public void testFillEventTimeFromProp(){
        serverConfiguration.setMqttEventTimeFromProp(mqttEventTimeFromProp);

        List<MqttProperties.StringPair> list = new ArrayList<>();
        long eventtime = System.currentTimeMillis();
        list.add(new MqttProperties.StringPair(mqttEventTimeFromProp, eventtime + ""));

        MqttProperties mp = new MqttProperties();
        mp.add(new MqttProperties.UserProperties(list));

        MessageImpl<byte[]> message = PulsarMessageConverter.toPulsarMsg(
                serverConfiguration, topic, mp, ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(eventtime, message.getEventTime());
    }

    @Test
    public void testFillMessageKeyFromProp(){
        String messageKey = "msgKey";
        serverConfiguration.setMqttMessageKeyFromProp(messageKey);

        List<MqttProperties.StringPair> list = new ArrayList<>();
        list.add(new MqttProperties.StringPair(messageKey, messageKey));

        MqttProperties mp = new MqttProperties();
        mp.add(new MqttProperties.UserProperties(list));

        MessageImpl<byte[]> message = PulsarMessageConverter.toPulsarMsg(
                serverConfiguration, topic, mp, ByteBuffer.wrap("world".getBytes()));
        Assert.assertEquals(messageKey, message.getKey());
    }

}
