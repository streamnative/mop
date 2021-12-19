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
package io.streamnative.pulsar.handlers.mqtt.mqtt3;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.base.provider.ClientToTopic;
import io.streamnative.pulsar.handlers.mqtt.client.MqttTestClient;
import io.streamnative.pulsar.handlers.mqtt.client.options.Optional;
import io.streamnative.pulsar.handlers.mqtt.client.options.PublishOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.SubscribeOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.UnSubscribeOptions;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
public class BasicIntegrationTest extends MQTTTestBase {

    @Test(dataProvider = "mqttClientWithPersistentTopicNames", timeOut = TIMEOUT)
    public void testBasicPublishAndConsumeWithMQTT(ClientToTopic clientToTopic) throws Exception {
        MqttTestClient client = clientToTopic.getClient();
        String topic = clientToTopic.getTopic();
        client.connect();
        client.subscribe(SubscribeOptions.builder()
                .topicFilters(topic)
                .qos(MqttQoS.AT_LEAST_ONCE)
                .build());
        byte[] msg = "payload".getBytes();
        client.publish(PublishOptions.builder()
                .topic(topic)
                .payload(msg)
                .qos(MqttQoS.AT_LEAST_ONCE)
                .build());
        Optional<MqttPublishMessage> receive = client.receive(3, TimeUnit.SECONDS);
        if (!receive.isSuccess()) {
            Assert.fail("fail");
        }
        MqttPublishMessage publishMessage = receive.getBody();
        byte[] bytes = new byte[publishMessage.payload().readableBytes()];
        ByteBuf payload = publishMessage.payload();
        payload.readBytes(bytes);
        Assert.assertEquals(publishMessage.variableHeader().topicName(), topic);
        Assert.assertEquals(bytes, msg);
        client.unsubscribe(UnSubscribeOptions.builder()
                .topicFilter(topic).build());
        client.disconnect();
    }

}
