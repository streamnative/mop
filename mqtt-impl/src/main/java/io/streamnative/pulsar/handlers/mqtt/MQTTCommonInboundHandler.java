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
package io.streamnative.pulsar.handlers.mqtt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.checkState;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTBrokerProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT common in bound handler.
 */
@Sharable
@Slf4j
public class MQTTCommonInboundHandler extends ChannelInboundHandlerAdapter {

    public static final String NAME = "InboundHandler";

    @Setter
    protected MQTTService mqttService;

    protected final ConcurrentHashMap<String, ProtocolMethodProcessor> processors = new ConcurrentHashMap<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        checkArgument(message instanceof MqttAdapterMessage);
        MqttAdapterMessage adapterMsg = (MqttAdapterMessage) message;
        MqttMessage mqttMessage = adapterMsg.getMqttMessage();
        ProtocolMethodProcessor processor = processors.computeIfAbsent(adapterMsg.getClientId(), key -> {
            MQTTBrokerProtocolMethodProcessor p = new MQTTBrokerProtocolMethodProcessor(mqttService, ctx);
            CompletableFuture<Void> inactiveFuture = p.getInactiveFuture();
            inactiveFuture.whenComplete((id, ex) -> {
                processors.remove(adapterMsg.getClientId());
            });
            return p;
        });
        try {
            checkState(mqttMessage);
            MqttMessageType messageType = mqttMessage.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("Inbound handler read message : type={}, clientId : {} adapter encodeType : {}", messageType,
                        adapterMsg.getClientId(), adapterMsg.getEncodeType());
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