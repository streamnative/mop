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
package io.streamnative.pulsar.handlers.mqtt.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.streamnative.pulsar.handlers.mqtt.support.MessageBuilder;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.codec.binary.Hex;

/**
 * Mqtt message utils.
 */
public class MqttMessageUtils {

    public static final int CLIENT_IDENTIFIER_MAX_LENGTH = 23;

    public static void checkState(MqttMessage msg) {
        if (!msg.decoderResult().isSuccess()) {
            throw new IllegalStateException(msg.decoderResult().cause().getMessage());
        }
    }

    public static MqttMessage pingResp() {
        MqttFixedHeader pingHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, AT_MOST_ONCE, false, 0);
        return new MqttMessage(pingHeader);
    }

    public static MqttMessage pingReq() {
        MqttFixedHeader pingHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, AT_MOST_ONCE, false, 0);
        return new MqttMessage(pingHeader);
    }

    public static String createClientIdentifier(Channel channel) {
        String clientIdentifier;
        if (channel != null && channel.remoteAddress() instanceof InetSocketAddress) {
            InetSocketAddress isa = (InetSocketAddress) channel.remoteAddress();
            clientIdentifier = Hex.encodeHexString(isa.getAddress().getAddress()) + Integer.toHexString(isa.getPort())
                    + Long.toHexString(System.currentTimeMillis() / 1000);
        } else {
            clientIdentifier = UUID.randomUUID().toString().replace("-", "");
        }
        if (clientIdentifier.length() > CLIENT_IDENTIFIER_MAX_LENGTH) {
            clientIdentifier = clientIdentifier.substring(0, CLIENT_IDENTIFIER_MAX_LENGTH);
        }
        return clientIdentifier;
    }

    public static MqttConnectMessage stuffClientIdToConnectMessage(MqttConnectMessage msg, String clientId) {
        MqttConnectPayload origin = msg.payload();
        MqttConnectPayload payload = new MqttConnectPayload(clientId, origin.willProperties(), origin.willTopic(),
                origin.willMessageInBytes(), origin.userName(), origin.passwordInBytes());
        return new MqttConnectMessage(msg.fixedHeader(), msg.variableHeader(), payload);
    }

    public static List<MqttTopicSubscription> topicSubscriptions(MqttSubscribeMessage msg) {
        List<MqttTopicSubscription> ackTopics = new ArrayList<>();

        for (MqttTopicSubscription req : msg.payload().topicSubscriptions()) {
            MqttQoS qos = req.qualityOfService();
            ackTopics.add(new MqttTopicSubscription(req.topicName(), qos));
        }
        return ackTopics;
    }

    public static WillMessage createWillMessage(MqttConnectMessage msg) {
        if (!msg.variableHeader().isWillFlag()) {
            return null;
        }
        final byte[] willMessage = msg.payload().willMessageInBytes();
        final String willTopic = msg.payload().willTopic();
        final boolean retained = msg.variableHeader().isWillRetain();
        final MqttQoS qos = MqttQoS.valueOf(msg.variableHeader().willQos());
        return new WillMessage(willTopic, willMessage, qos, retained);
    }

    public static RetainedMessage createRetainedMessage(MqttPublishMessage msg) {
        checkArgument(msg.fixedHeader().isRetain(), "Must be retained msg");
        final byte[] payload = new byte[msg.payload().readableBytes()];
        msg.payload().markReaderIndex();
        msg.payload().readBytes(payload);
        msg.payload().resetReaderIndex();
        final String topicName = msg.variableHeader().topicName();
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        return new RetainedMessage(topicName, payload, qos);
    }

    public static MqttPublishMessage createRetainedMessage(RetainedMessage msg) {
        checkArgument(msg != null, "Msg should not be null");
        return MessageBuilder.publish()
                .messageId(-1)
                .payload(Unpooled.copiedBuffer(msg.getPayload()))
                .topicName(msg.getTopic())
                .qos(msg.getQos())
                .retained(true)
                .build();
    }

    public static MqttPublishMessage createMqttWillMessage(WillMessage willMessage) {
        return MessageBuilder.publish()
                .topicName(willMessage.getTopic())
                .payload(Unpooled.copiedBuffer(willMessage.getWillMessage()))
                .qos(willMessage.getQos())
                .retained(willMessage.isRetained())
                .messageId(-1)
                .build();
    }
}
