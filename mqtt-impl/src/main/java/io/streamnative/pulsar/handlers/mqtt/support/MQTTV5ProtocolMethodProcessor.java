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
package io.streamnative.pulsar.handlers.mqtt.support;

import static io.netty.handler.codec.mqtt.MqttMessageType.CONNACK;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTDisconnectProtocolErrorException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTExceedServerReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTQosNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MQTTV5ProtocolMethodProcessor extends AbstractProtocolMethodProcessor {
    public MQTTV5ProtocolMethodProcessor(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService, ctx);
    }

    @Override
    void checkWillingMessageIfNeeded(String clientId, int willQos) {
        MqttQoS mqttQoS = MqttQoS.valueOf(willQos);
        if (mqttQoS == MqttQoS.FAILURE || mqttQoS == MqttQoS.EXACTLY_ONCE) {
            throw new MQTTQosNotSupportException(CONNACK, clientId, willQos);
        }
    }

    @Override
    Connection buildConnection(Connection.ConnectionBuilder connectionBuilder, MqttConnectMessage msg) {
        MqttProperties properties = msg.variableHeader().properties();
        Integer clientReceiveMaximum = MqttPropertyUtils.getReceiveMaximum(properties)
                .orElse(MqttPropertyUtils.MQTT5_DEFAULT_RECEIVE_MAXIMUM);
        MqttPropertyUtils.getExpireInterval(properties)
                .ifPresent(connectionBuilder::sessionExpireInterval);
        return connectionBuilder
                .serverReceivePubMaximum(configuration.getReceiveMaximum())
                .clientReceiveMaximum(clientReceiveMaximum)
                .build();
    }

    @Override
    void checkServerReceivePubMessageAndIncrementCounterIfNeeded(Channel channel, MqttPublishMessage msg) {
        Connection connection = NettyUtils.getConnection(channel);
        if (connection.getServerReceivePubMessage() >= connection.getServerReceivePubMaximum()) {
            throw new MQTTExceedServerReceiveMaximumException(msg.variableHeader().packetId(),
                    connection.getServerReceivePubMaximum());
        } else {
            connection.incrementServerReceivePubMessage();
        }
    }

    @Override
    void parseDisconnectPropertiesIfNeeded(MqttMessage msg) {
        // when reset expire interval present, we need to reset session expire interval.
        Object header = msg.variableHeader();
        Channel channel = serverCnx.ctx().channel();
        Connection connection = NettyUtils.getConnection(channel);
        if (header instanceof MqttReasonCodeAndPropertiesVariableHeader) {
            MqttProperties properties = ((MqttReasonCodeAndPropertiesVariableHeader) header).properties();
            checkAndProcessSessionInterval(connection, properties);
        }
    }

    private void checkAndProcessSessionInterval(Connection connection, MqttProperties properties) {
        Optional<Integer> expireInterval = MqttPropertyUtils.getExpireInterval(properties);
        String clientId = connection.getClientId();
        if (expireInterval.isPresent()) {
            Integer sessionExpireInterval = expireInterval.get();
            if (!connection.checkIsLegalExpireInterval(sessionExpireInterval)) {
                throw new MQTTDisconnectProtocolErrorException(clientId, sessionExpireInterval);
            }
            connection.updateSessionExpireInterval(sessionExpireInterval);
        }
    }
}
