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
package io.streamnative.pulsar.handlers.mqtt.client.v3x;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.Mqtt3ConnectBuilder;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAckReturnCode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.client.MqttTestClient;
import io.streamnative.pulsar.handlers.mqtt.client.options.ClientOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.ConnectOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.Optional;
import io.streamnative.pulsar.handlers.mqtt.client.options.PublishOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.SubscribeOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.UnSubscribeOptions;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class HiveMqV3TestClient implements MqttTestClient {
    private final Mqtt3BlockingClient client;
    private boolean autoAck;

    public HiveMqV3TestClient(ClientOptions clientOptions) {
        this.client = Mqtt3Client.builder()
                .serverHost(clientOptions.getServerUrl())
                .serverPort(clientOptions.getPort())
                .buildBlocking();
    }

    @Override
    public Optional<MqttConnAckMessage> connect() {
        return connect(ConnectOptions.builder().build());
    }

    @Override
    public Optional<MqttConnAckMessage> connect(ConnectOptions connectOptions) {
        Mqtt3ConnectBuilder.Send<Mqtt3ConnAck> mqtt3ConnBuilder = client.connectWith();
        if (connectOptions.getBasicAuth() != null){
            mqtt3ConnBuilder
                    .simpleAuth()
                    .username(connectOptions.getBasicAuth().getUsername())
                    .password(connectOptions.getBasicAuth().getPassword().getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth();
        }
        Mqtt3ConnAck connAck = mqtt3ConnBuilder
                .cleanSession(connectOptions.isCleanSession())
                .send();
        MqttConnAckMessage connAckMessage = MqttMessageBuilders.connAck()
                .returnCode(MqttConnectReturnCode.valueOf((byte) connAck.getReturnCode().getCode()))
                .sessionPresent(connAck.isSessionPresent())
                .build();
        return Optional.of(connAckMessage);
    }

    @Override
    public Optional<MqttSubAckMessage> subscribe(SubscribeOptions subscribeOptions) {
        this.autoAck = subscribeOptions.isAutoAck();
        Mqtt3SubAck ack = client.subscribeWith()
                .topicFilter(subscribeOptions.getTopicFilters())
                .qos(MqttQos.valueOf(subscribeOptions.getQos().name()))
                .send();
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdAndPropertiesVariableHeader mqttSubAckVariableHeader =
                new MqttMessageIdAndPropertiesVariableHeader(1, MqttProperties.NO_PROPERTIES);
        int[] codes = ack.getReturnCodes().stream()
                .mapToInt(Mqtt3SubAckReturnCode::getCode)
                .toArray();
        MqttSubAckPayload subAckPayload = new MqttSubAckPayload(codes);
        return Optional.of(new MqttSubAckMessage(mqttFixedHeader, mqttSubAckVariableHeader, subAckPayload));
    }

    @Override
    public Optional<MqttPublishMessage> receive(long time, TimeUnit unit) {
        try (Mqtt3BlockingClient.Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL, !autoAck)) {
            java.util.Optional<Mqtt3Publish> publish = publishes.receive(time, unit);
            if (!publish.isPresent()) {
                return Optional.fail();
            }
            Mqtt3Publish mqtt3Publish = publish.get();
            byte[] payloadAsBytes = mqtt3Publish.getPayloadAsBytes();
            if (payloadAsBytes == null) {
                return Optional.fail();
            }

            MqttPublishMessage publishMessage = MqttMessageBuilders
                    .publish()
                    .topicName(mqtt3Publish.getTopic().toString())
                    .messageId(0)
                    .qos(MqttQoS.valueOf(mqtt3Publish.getQos().getCode()))
                    .payload(Unpooled.buffer().writeBytes(payloadAsBytes))
                    .build();
            return Optional.of(publishMessage);
        } catch (InterruptedException e) {
            return Optional.fail();
        }
    }

    @Override
    public Optional<MqttUnsubAckMessage> unsubscribe(UnSubscribeOptions unSubscribeOptions) {
        client.unsubscribeWith()
                .topicFilter(unSubscribeOptions.getTopicFilter())
                .send();
        return Optional.unsupported();
    }

    @Override
    public Optional<MqttPubAckMessage> publish(PublishOptions publishOptions) {
        client.publishWith()
                .topic(publishOptions.getTopic())
                .payload(publishOptions.getPayload())
                .qos(MqttQos.valueOf(publishOptions.getQos().name()))
                .send();
        return Optional.unsupported();
    }

    @Override
    public Optional<MqttMessage> disconnect() {
        client.disconnect();
        return Optional.unsupported();
    }
}
