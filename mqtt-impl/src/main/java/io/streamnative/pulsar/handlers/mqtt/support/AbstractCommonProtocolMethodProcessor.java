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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandlerFactory;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Common protocol method processor.
 */
@Slf4j
public abstract class AbstractCommonProtocolMethodProcessor implements ProtocolMethodProcessor {

    protected final Channel channel;

    protected final MQTTAuthenticationService authenticationService;

    private final boolean authenticationEnabled;


    public AbstractCommonProtocolMethodProcessor(MQTTAuthenticationService authenticationService,
                                                 boolean authenticationEnabled,
                                                 ChannelHandlerContext ctx) {
        this.authenticationService = authenticationService;
        this.authenticationEnabled = authenticationEnabled;
        this.channel = ctx.channel();
    }

    public abstract void initConnection(MqttConnectMessage msg,
                                        String clientId, String userRole, AckHandler ackHandler);

    @Override
    public void processConnect(MqttConnectMessage connectMessage) {
        MqttConnectPayload payload = connectMessage.payload();
        final int protocolVersion = connectMessage.variableHeader().version();
        final String username = payload.userName();
        String clientId = payload.clientIdentifier();
        if (log.isDebugEnabled()) {
            log.debug("process CONNECT message. CId={}, username={}", clientId, username);
        }
        // Check MQTT protocol version.
        if (!MqttUtils.isSupportedVersion(protocolVersion)) {
            log.error("MQTT protocol version is not valid. CId={}", clientId);
            channel.writeAndFlush(MqttMessageBuilders.connAck().sessionPresent(false)
                    .returnCode(Mqtt5ConnReasonCode.UNSUPPORTED_PROTOCOL_VERSION.convertToNettyKlass()));
            channel.close();
            return;
        }
        // when mqtt protocol valid.
        AckHandler ackHandler = AckHandlerFactory.of(protocolVersion).getAckHandler();
        if (!MqttUtils.isQosSupported(connectMessage)) {
            ackHandler.sendConnNotSupported(channel);
            return;
        }
        // Client must specify the client ID except enable clean session on the connection.
        if (StringUtils.isEmpty(clientId)) {
            if (!connectMessage.variableHeader().isCleanSession()) {
                ackHandler.sendConnClientIdentifierInvalid(channel);
                return;
            }
            clientId = MqttMessageUtils.createClientIdentifier(channel);
            if (log.isDebugEnabled()) {
                log.debug("Client has connected with generated identifier. CId={}", clientId);
            }
        }
        String userRole = null;
        if (!authenticationEnabled) {
            log.info("Authentication is disabled, allowing client. CId={}, username={}", clientId, username);
        } else {
            MQTTAuthenticationService.AuthenticationResult authResult = authenticationService.authenticate(payload);
            if (authResult.isFailed()) {
                ackHandler.sendConnAuthenticationFail(channel);
                return;
            }
            userRole = authResult.getUserRole();
        }
        initConnection(connectMessage, clientId, userRole, ackHandler);
    }

    @Override
    public void processPubAck(MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubRel(MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRel] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubRec(MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubComp(MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }
}
