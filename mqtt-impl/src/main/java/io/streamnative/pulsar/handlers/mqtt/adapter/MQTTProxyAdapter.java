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
package io.streamnative.pulsar.handlers.mqtt.adapter;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.checkState;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * Proxy exchanger is the bridge between proxy and MoP.
 */
@Slf4j
public class MQTTProxyAdapter {

    private final DefaultThreadFactory threadFactory = new DefaultThreadFactory("mqtt-proxy-adapter");
    private final MQTTProxyService proxyService;
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final ConcurrentMap<InetSocketAddress, Channel> channels;
    private final int workerThread = Runtime.getRuntime().availableProcessors();

    public MQTTProxyAdapter(MQTTProxyService proxyService) {
        this.proxyService = proxyService;
        this.channels = new ConcurrentHashMap<>();
        this.bootstrap = new Bootstrap();
        this.eventLoopGroup = EventLoopUtil.newEventLoopGroup(workerThread, false, threadFactory);
        this.bootstrap.group(eventLoopGroup)
                .channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        int maxBytesInMessage = proxyService.getProxyConfig().getMqttMessageMaxLength();
                        ch.pipeline().addLast("adapter-decoder", new MqttAdapterDecoder(maxBytesInMessage));
                        ch.pipeline().addLast("adapter-encoder", MqttAdapterEncoder.INSTANCE);
                        ch.pipeline().addLast("adapter-handler", new AdapterHandler());
                    }
                });
    }

    public AdapterChannel getAdapterChannel(InetSocketAddress host) {
        return new AdapterChannel(getChannel(host));
    }

    private Channel getChannel(InetSocketAddress host) {
        Channel channel = channels.get(host);
        if (channel == null || !channel.isActive()) {
            return createNewChannel(host);
        }
        return channel;
    }

    private Channel createNewChannel(InetSocketAddress host) {
        ChannelFuture future;
        try {
            synchronized (bootstrap) {
                future = bootstrap.connect(host).await();
            }
        } catch (Exception e) {
            log.error(String.format("Connect to : %s error.", host), e);
            return null;
        }
        if (future.isSuccess()) {
            Channel channel = future.channel();
            channels.put(host, channel);
            return channel;
        } else {
            log.error(String.format("Connect to : %s failed.", host), future.cause());
            return null;
        }
    }

    public void shutdown() {
        try {
            closeChannels();
            this.eventLoopGroup.shutdownGracefully();
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception", e);
        }
    }

    private void closeChannels() {
        for (Channel cw : this.channels.values()) {
            cw.close();
        }
        this.channels.clear();
    }

    private class AdapterHandler extends ChannelInboundHandlerAdapter{

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
            checkArgument(message instanceof MqttAdapterMessage);
            MqttAdapterMessage adapterMsg = (MqttAdapterMessage) message;
            MqttMessageType messageType = adapterMsg.getMqttMessage().fixedHeader().messageType();
            String clientId = adapterMsg.getClientId();
            MqttMessage msg = adapterMsg.getMqttMessage();
            Connection connection = proxyService.getConnectionManager().getConnection(clientId);
            if (connection == null) {
                log.warn("Not find matched connection : {}, message : {}", clientId, adapterMsg);
                return;
            }
            MQTTProxyProtocolMethodProcessor processor =
                    ((MQTTProxyProtocolMethodProcessor) connection.getProcessor());
            try {
                checkState(msg);
                if (log.isDebugEnabled()) {
                    log.debug("channelRead messageType {}", messageType);
                }
                switch (messageType) {
                    case DISCONNECT:
                        if (MqttUtils.isMqtt5(connection.getProtocolVersion())) {
                            connection.getChannel().writeAndFlush(msg);
                        } else {
                            connection.getChannel().close();
                        }
                        break;
                    case PUBLISH:
                        MqttPublishMessage pubMessage = (MqttPublishMessage) msg;
                        int packetId = pubMessage.variableHeader().packetId();
                        String topicName = pubMessage.variableHeader().topicName();
                        processor.getPacketIdTopic().put(packetId, topicName);
                        processor.getChannel().writeAndFlush(msg);
                        break;
                    case CONNACK:
                        break;
                    case SUBACK:
                        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) msg;
                        if (processor.checkIfSendSubAck(subAckMessage.variableHeader().messageId())) {
                            processor.getChannel().writeAndFlush(msg);
                        }
                        break;
                    default:
                        processor.getChannel().writeAndFlush(msg);
                        break;
                }
            } catch (Throwable ex) {
                log.error("Exception was caught while processing MQTT broker message", ex);
                ctx.close();
                processor.getChannel().close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("exception caught when connect with MoP broker.", cause);
            ctx.close();
        }
    }
}
