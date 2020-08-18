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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;
import static io.netty.handler.codec.mqtt.MqttMessageType.CONNACK;


// proxy -> MoP
/**
 * Proxy handler is the bridge between proxy and MoP.
 */
@Slf4j
public class ProxyHandler {
    private final ProtocolMethodProcessor processor;
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    // client -> proxy
    private Channel clientChannel;
    @Getter
    // proxy -> MoP
    private Channel brokerChannel;
    private State state;
    private List<Object> connectMsgList;

    ProxyHandler(ProxyService proxyService, ProxyConnection proxyConnection,
                 String mqttBrokerHost, int mqttBrokerPort, List<Object> connectMsgList) throws Exception {
        this.proxyService = proxyService;
        this.proxyConnection = proxyConnection;
        clientChannel = this.proxyConnection.getCnx().channel();
        this.connectMsgList = connectMsgList;
        processor = new ProxyInboundHandler(proxyService, proxyConnection);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientChannel.eventLoop())
                .channel(clientChannel.getClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("frameEncoder", MqttEncoder.INSTANCE);
                        ch.pipeline().addLast("processor", new ProxyBackendHandler());
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
        state = State.Init;
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
            this.cnx = ctx;
            super.channelActive(ctx);
            for (Object msg : connectMsgList) {
                ((ByteBuf) msg).retain();
                brokerChannel.writeAndFlush(msg).syncUninterruptibly();
            }
            brokerChannel.read();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {

            switch (state) {
                case Init:
                    MqttMessage msg = (MqttMessage) message;
                    MqttMessageType messageType = msg.fixedHeader().messageType();
                    if (log.isDebugEnabled()) {
                        log.info("Processing MQTT message, type={}", messageType);
                    }

                    if (messageType == CONNACK) {
                        checkState(msg instanceof MqttConnAckMessage);
                        state = State.Connected;
                    }
                case Failed:
                    Channel nettyChannel = ctx.channel();
                    checkState(nettyChannel.equals(this.cnx.channel()));
                    break;
                case Connected:
                    clientChannel.writeAndFlush(message);
                    break;
                case Closed:
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            state = State.Failed;
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

}
