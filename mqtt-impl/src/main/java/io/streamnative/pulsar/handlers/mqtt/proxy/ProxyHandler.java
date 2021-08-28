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
import static io.netty.handler.codec.mqtt.MqttMessageType.CONNACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.SUBACK;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy handler is the bridge between proxy and MoP.
 */
@Slf4j
public class ProxyHandler {
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    @Getter
    // client -> proxy
    private Channel clientChannel;
    @Getter
    // proxy -> MoP
    private Channel brokerChannel;
    private State state = State.Init;
    private List<Object> connectMsgList;
    private CompletableFuture<Void> brokerFuture = new CompletableFuture<>();

    ProxyHandler(ProxyService proxyService, ProxyConnection proxyConnection,
                 String mqttBrokerHost, int mqttBrokerPort, List<Object> connectMsgList) throws Exception {
        this.proxyService = proxyService;
        this.proxyConnection = proxyConnection;
        clientChannel = this.proxyConnection.getCnx().channel();
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
                        ch.pipeline().addLast("handler", new ProxyBackendHandler());
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(mqttBrokerHost, mqttBrokerPort);
        brokerChannel = channelFuture.channel();
        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                clientChannel.close();
            }
        });
        log.info("Broker channel connect. broker: {}:{}, isOpen: {}",
                mqttBrokerHost, mqttBrokerPort, brokerChannel.isOpen());
    }

    private class ProxyBackendHandler extends ChannelInboundHandlerAdapter implements FutureListener<Void> {

        private ChannelHandlerContext cnx;

        ProxyBackendHandler() {}

        @Override
        public void operationComplete(Future future) throws Exception {
            // This is invoked when the write operation on the paired connection
            // is completed
            if (future.isSuccess()) {
                brokerChannel.read();
            } else {
                log.warn("[{}] [{}] Failed to write on proxy connection. Closing both connections.", clientChannel,
                        brokerChannel, future.cause());
                clientChannel.close();
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("proxy handler active: {}", connectMsgList);
            this.cnx = ctx;
            super.channelActive(ctx);
            for (Object msg : connectMsgList) {
                brokerChannel.writeAndFlush(msg).syncUninterruptibly();
            }
            brokerChannel.read();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
            log.info("channel read: {}", message);
            if (message instanceof MqttMessage && ((MqttMessage) message).decoderResult().isFailure()) {
                log.error("Failed to decode mqttMessage.", ((MqttMessage) message).decoderResult().cause());
            }
            switch (state) {
                case Init:
                    MqttMessage msg = (MqttMessage) message;
                    MqttMessageType messageType = msg.fixedHeader().messageType();
                    if (log.isDebugEnabled()) {
                        log.info("Processing Proxy Handler message, type={}", messageType);
                    }

                    if (messageType == CONNACK) {
                        log.info("The messageType is CONNACK, set the state to Connected.");
                        checkState(msg instanceof MqttConnAckMessage);
                        state = State.Connected;
                        brokerFuture.complete(null);
                    }
                    break;
                case Failed:
                    Channel nettyChannel = ctx.channel();
                    checkState(nettyChannel.equals(this.cnx.channel()));
                    break;
                case Connected:
                    log.info("channelRead Connected: {}", message);
                    msg = (MqttMessage) message;
                    messageType = msg.fixedHeader().messageType();
                    if (messageType == SUBACK) {
                        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) message;
                        if (proxyConnection.decreaseSubscribeTopicsCount(
                                subAckMessage.variableHeader().messageId()) == 0) {
                            clientChannel.writeAndFlush(message);
                        }
                    } else {
                        clientChannel.writeAndFlush(message);
                    }
                    break;
                case Closed:
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("Failed to create connection with MoP broker.", cause);
            state = State.Failed;
            brokerFuture.completeExceptionally(cause);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            proxyConnection.close();
        }
    }

    public void close() {
        state = State.Closed;
        this.brokerChannel.close();
    }

    enum State {
        Init,
        Connected,
        Failed,
        Closed
    }

    public CompletableFuture<Void> brokerFuture() {
        return this.brokerFuture;
    }

}
