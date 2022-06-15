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
package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PayloadFormatIndicator;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class MQTT5ConnectRelatedProtocolTest extends MQTTTestBase {

    @Test
    public void testConnectWillMessage() throws Exception {
        final String topic = "will-topic-test";
        Mqtt5BlockingClient client1 = Mqtt5Client.builder()
                .identifier("client-1")
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client1.connectWith()
                .send();
        client1.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client1.publishes(MqttGlobalPublishFilter.ALL);
        //
        Mqtt5BlockingClient client2 = Mqtt5Client.builder()
                .identifier("client-2")
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();

        client2.connectWith()
                .willPublish()
                .topic(topic)
                .payload("will-message".getBytes(StandardCharsets.UTF_8))
                .contentType("will-content-type")
                .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.UTF_8)
                .correlationData("cd".getBytes(StandardCharsets.UTF_8))
                .responseTopic("will-response-topic")
                .payloadFormatIndicator(Mqtt5PayloadFormatIndicator.fromCode(1))
                .applyWillPublish()
                .send();
        client2.disconnect();
        //
        Mqtt5Publish message = publishes.receive();
        Assert.assertNotNull(message);
        Assert.assertEquals(new String(message.getPayloadAsBytes()), "will-message");
        // Validate the user properties order, must be the same with set order.
        ByteBuffer byteBuffer = message.getCorrelationData().get();
        Assert.assertNotNull(byteBuffer);
        byte[] bytes;
        if (byteBuffer.hasArray()) {
            bytes = byteBuffer.array();
        } else {
            bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
        }
        String result = new String(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(result, "cd");

        Assert.assertNotNull(message.getResponseTopic().get());
        Assert.assertEquals(message.getResponseTopic().get().toString(), "will-response-topic");
        Assert.assertNotNull(message.getContentType().get());
        Assert.assertEquals(message.getContentType().get().toString(), "will-content-type");
        Assert.assertNotNull(message.getPayloadFormatIndicator().get());
        Assert.assertEquals(message.getPayloadFormatIndicator().get(), Mqtt5PayloadFormatIndicator.UTF_8);

        publishes.close();
        client1.disconnect();
    }
}
