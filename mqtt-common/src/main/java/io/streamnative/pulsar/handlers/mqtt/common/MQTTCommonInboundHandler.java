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
package io.streamnative.pulsar.handlers.mqtt.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils.checkState;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttConnectAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttDisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.common.utils.NettyUtils;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT common in bound handler.
 */
@Sharable
@Slf4j
public class MQTTCommonInboundHandler extends ChannelInboundHandlerAdapter {

    public static final String NAME = "InboundHandler";

    protected final ConcurrentHashMap<String, ProtocolMethodProcessor> processors = new ConcurrentHashMap<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        checkArgument(message instanceof MqttAdapterMessage);
        MqttAdapterMessage adapterMsg = (MqttAdapterMessage) message;
        MqttMessage mqttMessage = adapterMsg.getMqttMessage();
        final ProtocolMethodProcessor processor = processors.get(adapterMsg.getClientId());
        try {
            checkNotNull(processor);
            checkState(mqttMessage);
            MqttMessageType messageType = mqttMessage.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("Inbound handler read message : type={}, clientId : {} adapter encodeType : {}", messageType,
                        adapterMsg.getClientId(), adapterMsg.getEncodeType());
            }
            if (messageType != MqttMessageType.CONNECT && !processor.connectionEstablished()) {
                if (adapterMsg.fromProxy()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Ignore inflating message from proxy. message:{}",
                                ctx.channel().remoteAddress(), mqttMessage);
                    }
                    ReferenceCountUtil.safeRelease(mqttMessage); // release publish packet
                    return;
                } else {
                    log.warn("[{}] Receive message before connect. message:{}",
                            ctx.channel().remoteAddress(), mqttMessage);
                    ReferenceCountUtil.safeRelease(mqttMessage); // release publish packet
                    ctx.channel().close();
                }
            }
            switch (messageType) {
                case CONNECT:
                    processor.processConnect(adapterMsg);
                    break;
                case SUBSCRIBE:
                    processor.processSubscribe(adapterMsg);
                    break;
                case UNSUBSCRIBE:
                    processor.processUnSubscribe(adapterMsg);
                    break;
                case PUBLISH:
                    processor.processPublish(adapterMsg);
                    break;
                case PUBREC:
                    processor.processPubRec(adapterMsg);
                    break;
                case PUBCOMP:
                    processor.processPubComp(adapterMsg);
                    break;
                case PUBREL:
                    processor.processPubRel(adapterMsg);
                    break;
                case DISCONNECT:
                    processor.processDisconnect(adapterMsg);
                    break;
                case PUBACK:
                    processor.processPubAck(adapterMsg);
                    break;
                case PINGREQ:
                    processor.processPingReq(adapterMsg);
                    break;
                case AUTH:
                    processor.processAuthReq(adapterMsg);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown MessageType: " + messageType);
            }
        } catch (IllegalStateException ex) {
            ReferenceCountUtil.safeRelease(mqttMessage);
            log.warn("Invalid MQTT message state: {}", ex.getMessage());

            int protocolVersion = 4;
            try {
                Connection existingConnection = NettyUtils.getConnection(ctx.channel());
                if (existingConnection != null) {
                    protocolVersion = existingConnection.getProtocolVersion();
                }
            } catch (Exception e) {
            }

            MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();
            if (mqttMessageType == MqttMessageType.CONNECT) {
                MqttConnectVariableHeader connectVariableHeader =
                    (MqttConnectVariableHeader) mqttMessage.variableHeader();
                protocolVersion = connectVariableHeader.version();

                // For CONNECT message errors, send a CONNACK error response.
                MqttMessage errorResponse = MqttConnectAck.errorBuilder().protocolError(protocolVersion);
                MqttAdapterMessage errorAdapterMsg = new MqttAdapterMessage(
                        adapterMsg.getClientId(),
                        errorResponse,
                        adapterMsg.fromProxy()
                );
                ctx.writeAndFlush(errorAdapterMsg);
            } else {
                // For other message errors, send a DISCONNECT error response if supported.
                MqttAck errorAck = MqttDisconnectAck.errorBuilder(protocolVersion)
                        .reasonCode(Mqtt5DisConnReasonCode.MALFORMED_PACKET)
                        .reasonString("Invalid message format: " + ex.getMessage())
                        .build();

                if (errorAck.isProtocolSupported()) {
                    MqttAdapterMessage errorAdapterMsg = new MqttAdapterMessage(
                            adapterMsg.getClientId(),
                            errorAck.getMqttMessage(),
                            adapterMsg.fromProxy()
                    );
                    ctx.writeAndFlush(errorAdapterMsg);
                }
            }
            ctx.close();
        } catch (Throwable ex) {
            ReferenceCountUtil.safeRelease(mqttMessage);
            log.error("Exception was caught while processing MQTT message, ", ex);
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        processors.values().forEach(ProtocolMethodProcessor::processConnectionLost);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn(
                "An unexpected exception was caught while processing MQTT message. "
                        + "Closing Netty channel {}. connection = {}",
                ctx.channel(),
                NettyUtils.getConnection(ctx.channel()),
                cause);
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;
            if (e.state() == IdleState.ALL_IDLE) {
                log.warn("close connection : {} due to reached all idle time", NettyUtils.getConnection(ctx.channel()));
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            ctx.channel().flush();
        }
        ctx.fireChannelWritabilityChanged();
    }
}