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
import static com.google.common.base.Preconditions.checkNotNull;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.checkState;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT common in bound handler.
 */
@Sharable
@Slf4j
public class MQTTCommonInboundHandler extends ChannelInboundHandlerAdapter {

    protected ProtocolMethodProcessor processor;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        checkArgument(message instanceof MqttMessage);
        checkNotNull(processor);
        MqttMessage msg = (MqttMessage) message;
        try {
            checkState(msg);
            MqttMessageType messageType = msg.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("Processing MQTT Inbound handler message, type={}", messageType);
            }
            switch (messageType) {
                case CONNECT:
                    checkArgument(msg instanceof MqttConnectMessage);
                    processor.processConnect((MqttConnectMessage) msg);
                    break;
                case SUBSCRIBE:
                    checkArgument(msg instanceof MqttSubscribeMessage);
                    processor.processSubscribe((MqttSubscribeMessage) msg);
                    break;
                case UNSUBSCRIBE:
                    checkArgument(msg instanceof MqttUnsubscribeMessage);
                    processor.processUnSubscribe((MqttUnsubscribeMessage) msg);
                    break;
                case PUBLISH:
                    checkArgument(msg instanceof MqttPublishMessage);
                    processor.processPublish((MqttPublishMessage) msg);
                    break;
                case PUBREC:
                    processor.processPubRec(msg);
                    break;
                case PUBCOMP:
                    processor.processPubComp(msg);
                    break;
                case PUBREL:
                    processor.processPubRel(msg);
                    break;
                case DISCONNECT:
                    processor.processDisconnect(msg);
                    break;
                case PUBACK:
                    checkArgument(msg instanceof MqttPubAckMessage);
                    processor.processPubAck((MqttPubAckMessage) msg);
                    break;
                case PINGREQ:
                    processor.processPingReq();
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown MessageType: " + messageType);
            }
        } catch (Throwable ex) {
            ReferenceCountUtil.safeRelease(msg);
            log.error("Exception was caught while processing MQTT message, ", ex);
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        processor.processConnectionLost();
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