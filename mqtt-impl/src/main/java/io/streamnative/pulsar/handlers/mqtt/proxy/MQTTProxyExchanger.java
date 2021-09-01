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

import static com.google.common.base.Preconditions.checkState;
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
import io.netty.handler.timeout.IdleStateHandler;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Proxy exchanger is the bridge between proxy and MoP.
 */
@Slf4j
public class MQTTProxyExchanger {

    private MQTTProxyHandler proxyHandler;
    @Getter
    // client -> proxy
    private Channel clientChannel;
    @Getter
    // proxy -> MoP
    private Channel brokerChannel;
    private List<MqttConnectMessage> connectMsgList;
    private CompletableFuture<Void> brokerFuture = new CompletableFuture<>();

    MQTTProxyExchanger(MQTTProxyHandler proxyHandler, Pair<String, Integer> brokerHostAndPort,
                       List<MqttConnectMessage> connectMsgList) throws Exception {
        this.proxyHandler = proxyHandler;
        clientChannel = this.proxyHandler.getCnx().channel();
        this.connectMsgList = connectMsgList;

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientChannel.eventLoop())
                .channel(clientChannel.getClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(10, 0, 0));
                        ch.pipeline().addLast("decoder", new MqttDecoder());
                        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
                        ch.pipeline().addLast("handler", new ExchangerHandler());
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(brokerHostAndPort.getLeft(), brokerHostAndPort.getRight());
        brokerChannel = channelFuture.channel();
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                log.info("connected to broker: {}", brokerHostAndPort);
            }
        });
    }

    private class ExchangerHandler extends ChannelInboundHandlerAdapter{

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            for (MqttConnectMessage msg : connectMsgList) {
                brokerChannel.writeAndFlush(msg);
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
            checkState(message instanceof MqttMessage);
            MqttMessage msg = (MqttMessage) message;
            MqttMessageType messageType = msg.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("channelRead messageType {}", messageType);
            }
            switch (messageType) {
                case CONNACK:
                    brokerFuture.complete(null);
                    break;
                case SUBACK:
                    MqttSubAckMessage subAckMessage = (MqttSubAckMessage) message;
                    if (proxyHandler.decreaseSubscribeTopicsCount(
                            subAckMessage.variableHeader().messageId()) == 0) {
                        clientChannel.writeAndFlush(message);
                    }
                    break;
                default:
                    clientChannel.writeAndFlush(message);
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("failed to create connection with MoP broker.", cause);
            brokerFuture.completeExceptionally(cause);
        }
    }

    public void close() {
        this.brokerChannel.close();
    }

    public CompletableFuture<Void> brokerFuture() {
        return this.brokerFuture;
    }
}
