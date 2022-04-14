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
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.restrictions.InvalidReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
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

    protected final ChannelHandlerContext ctx;
    protected final Channel channel;

    protected final MQTTAuthenticationService authenticationService;

    private final boolean authenticationEnabled;

    public AbstractCommonProtocolMethodProcessor(MQTTAuthenticationService authenticationService,
                                                 boolean authenticationEnabled,
                                                 ChannelHandlerContext ctx) {
        this.authenticationService = authenticationService;
        this.authenticationEnabled = authenticationEnabled;
        this.ctx = ctx;
        this.channel = ctx.channel();
    }

    public abstract void doProcessConnect(MqttAdapterMessage msg, String userRole, ClientRestrictions restrictions);

    @Override
    public void processConnect(MqttAdapterMessage adapter) {
        MqttConnectMessage msg = (MqttConnectMessage) adapter.getMqttMessage();
        MqttConnectPayload payload = msg.payload();
        MqttConnectMessage connectMessage = msg;
        final int protocolVersion = msg.variableHeader().version();
        final String username = payload.userName();
        String clientId = payload.clientIdentifier();
        if (log.isDebugEnabled()) {
            log.debug("[CONNECT] process CONNECT message. CId={}, username={}", clientId, username);
        }
        // Check MQTT protocol version.
        if (!MqttUtils.isSupportedVersion(protocolVersion)) {
            log.error("[CONNECT] MQTT protocol version is not valid. CId={}", clientId);
            channel.writeAndFlush(MqttConnectAckHelper.errorBuilder().unsupportedVersion());
            channel.close();
            return;
        }
        if (!MqttUtils.isQosSupported(msg)) {
            channel.writeAndFlush(MqttConnectAckHelper.errorBuilder().willQosNotSupport(protocolVersion));
            channel.close();
            return;
        }
        // Client must specify the client ID except enable clean session on the connection.
        if (StringUtils.isEmpty(clientId)) {
            if (!msg.variableHeader().isCleanSession()) {
                channel.writeAndFlush(MqttConnectAckHelper.errorBuilder().identifierInvalid(protocolVersion));
                channel.close();
                log.error("[CONNECT] The MQTT client ID cannot be empty. Username={}", username);
                return;
            }
            clientId = MqttMessageUtils.createClientIdentifier(channel);
            connectMessage = MqttMessageUtils.stuffClientIdToConnectMessage(msg, clientId);
            if (log.isDebugEnabled()) {
                log.debug("[CONNECT] Client has connected with generated identifier. CId={}", clientId);
            }
        }
        String userRole = null;
        if (!authenticationEnabled) {
            if (log.isDebugEnabled()) {
                log.debug("[CONNECT] Authentication is disabled, allowing client. CId={}, username={}",
                        clientId, username);
            }
        } else {
            MQTTAuthenticationService.AuthenticationResult authResult = authenticationService.authenticate(payload);
            if (authResult.isFailed()) {
                channel.writeAndFlush(MqttConnectAckHelper.errorBuilder().authFail(protocolVersion));
                channel.close();
                log.error("[CONNECT] Invalid or incorrect authentication. CId={}, username={}", clientId, username);
                return;
            }
            userRole = authResult.getUserRole();
        }
        try {
            ClientRestrictions.ClientRestrictionsBuilder clientRestrictionsBuilder = ClientRestrictions.builder();
            MqttPropertyUtils.parsePropertiesToStuffRestriction(clientRestrictionsBuilder, msg);
            clientRestrictionsBuilder
                    .keepAliveTime(msg.variableHeader().keepAliveTimeSeconds())
                    .cleanSession(msg.variableHeader().isCleanSession());
            doProcessConnect(adapter.isAdapter() ? adapter : new MqttAdapterMessage(clientId, connectMessage), userRole,
                    clientRestrictionsBuilder.build());
        } catch (InvalidReceiveMaximumException invalidReceiveMaximumException) {
            channel.writeAndFlush(MqttConnectAckHelper.errorBuilder().protocolError(protocolVersion));
            channel.close();
            log.error("[CONNECT] Fail to parse receive maximum because of zero value, CId={}", clientId);
        }
    }

    @Override
    public void processPubAck(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubRel(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRel] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubRec(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }

    @Override
    public void processPubComp(MqttAdapterMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
    }
}
