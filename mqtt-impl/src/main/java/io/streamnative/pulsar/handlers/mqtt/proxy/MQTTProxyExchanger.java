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
package io.streamnative.pulsar.handlers.mqtt.proxy;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.checkState;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy exchanger is the bridge between proxy and MoP.
 */
@Slf4j
public class MQTTProxyExchanger {

    private MQTTProxyProtocolMethodProcessor processor;

    private Channel brokerChannel;
    private CompletableFuture<Void> brokerConnected = new CompletableFuture<>();
    private CompletableFuture<Void> brokerConnectedAck = new CompletableFuture<>();

    MQTTProxyExchanger(MQTTProxyProtocolMethodProcessor processor, InetSocketAddress mqttBroker) {
        this.processor = processor;
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(processor.getChannel().eventLoop())
                .channel(processor.getChannel().getClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("decoder", new MqttDecoder());
                        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
                        ch.pipeline().addLast("handler", new ExchangerHandler());
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(mqttBroker.getHostName(), mqttBroker.getPort());
        brokerChannel = channelFuture.channel();
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                brokerConnected.complete(null);
                log.info("connected to broker: {}", mqttBroker);
            } else {
                brokerConnected.completeExceptionally(future.cause());
            }
        });
    }

    private class ExchangerHandler extends ChannelInboundHandlerAdapter{

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            NettyUtils.setClientId(ctx.channel(), NettyUtils.getClientId(processor.getChannel()));
            MqttConnectMessage connectMessage = NettyUtils.getConnectMsg(processor.getChannel());
            ctx.channel().writeAndFlush(connectMessage);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
            checkArgument(message instanceof MqttMessage);
            MqttMessage msg = (MqttMessage) message;
            try {
                checkState(msg);
                MqttMessageType messageType = msg.fixedHeader().messageType();
                if (log.isDebugEnabled()) {
                    log.debug("channelRead messageType {}", messageType);
                }
                switch (messageType) {
                    case CONNACK:
                        brokerConnectedAck.complete(null);
                        break;
                    case SUBACK:
                        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) message;
                        if (processor.decreaseSubscribeTopicsCount(
                                subAckMessage.variableHeader().messageId()) == 0) {
                            processor.getChannel().writeAndFlush(message);
                        }
                        break;
                    default:
                        processor.getChannel().writeAndFlush(message);
                        break;
                }
            } catch (Throwable ex) {
                log.error("Exception was caught while processing MQTT broker message", ex);
                brokerConnectedAck.completeExceptionally(ex);
                ctx.close();
                processor.getChannel().close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("exception caught when connect with MoP broker.", cause);
            ctx.close();
            processor.getChannel().close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.error("proxy to broker channel inactive. Cid = {}", NettyUtils.getClientId(ctx.channel()));
            processor.getChannel().close();
        }
    }

    public void close() {
        this.brokerChannel.close();
    }

    public CompletableFuture<Void> connectedAck() {
        return brokerConnected.thenCompose(__ -> brokerConnectedAck);
    }

    public boolean isWritable() {
        return this.brokerChannel.isWritable();
    }

    public void writeAndFlush(Object msg) {
        this.brokerChannel.writeAndFlush(msg);
    }
}
