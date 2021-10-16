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

import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
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

    public static MqttConnAckMessage connAck(MqttConnectReturnCode returnCode) {
        return connAck(returnCode, false);
    }

    public static MqttConnAckMessage connAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(returnCode, sessionPresent);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    public static MqttPubAckMessage pubAck(int packetId) {
        return new MqttPubAckMessage(new MqttFixedHeader(MqttMessageType.PUBACK, false, AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId));
    }

    public static MqttMessage pingResp() {
        MqttFixedHeader pingHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, AT_MOST_ONCE, false, 0);
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

    public static MqttConnectMessage createMqttConnectMessage(MqttConnectMessage msg, String clientId) {
        MqttConnectPayload origin = msg.payload();
        MqttConnectPayload payload = new MqttConnectPayload(clientId, origin.willProperties(), origin.willTopic(),
                origin.willMessageInBytes(), origin.userName(), origin.passwordInBytes());
        return new MqttConnectMessage(msg.fixedHeader(), msg.variableHeader(), payload);
    }

    public static int getKeepAliveTime(MqttConnectMessage msg) {
        return Math.round(msg.variableHeader().keepAliveTimeSeconds() * 1.5f);
    }

    public static List<MqttTopicSubscription> topicSubscriptions(MqttSubscribeMessage msg) {
        List<MqttTopicSubscription> ackTopics = new ArrayList<>();

        for (MqttTopicSubscription req : msg.payload().topicSubscriptions()) {
            MqttQoS qos = req.qualityOfService();
            ackTopics.add(new MqttTopicSubscription(req.topicName(), qos));
        }
        return ackTopics;
    }

    public static MqttSubAckMessage createSubAckMessage(List<MqttTopicSubscription> topicFilters, int messageId) {
        List<Integer> grantedQoSLevels = new ArrayList<>();
        for (MqttTopicSubscription req : topicFilters) {
            grantedQoSLevels.add(req.qualityOfService().value());
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, AT_MOST_ONCE,
                false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQoSLevels);
        return new MqttSubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(messageId), payload);
    }
}
