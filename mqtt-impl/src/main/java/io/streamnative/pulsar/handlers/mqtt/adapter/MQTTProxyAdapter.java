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
import static org.apache.pulsar.client.util.MathUtils.signSafeMod;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.ScheduledFuture;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.netty.ChannelFutures;
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
    private final AtomicLong counter = new AtomicLong(0);
    @Getter
    private final ConcurrentMap<InetSocketAddress, Map<Integer, CompletableFuture<Channel>>> pool;
    private final int workerThread = Runtime.getRuntime().availableProcessors();
    private final int maxNoOfChannels;

    private ScheduledFuture<?> scheduledFuture;

    public MQTTProxyAdapter(MQTTProxyService proxyService) {
        this.proxyService = proxyService;
        this.pool = new ConcurrentHashMap<>();
        this.bootstrap = new Bootstrap();
        this.eventLoopGroup = EventLoopUtil.newEventLoopGroup(workerThread, false, threadFactory);
        this.maxNoOfChannels = proxyService.getProxyConfig().getMaxNoOfChannels();
        this.bootstrap.group(eventLoopGroup)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, proxyService.getProxyConfig().getConnectTimeoutMs())
                .channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        int maxBytesInMessage = proxyService.getProxyConfig().getMqttMessageMaxLength();
                        ch.pipeline().addLast(MqttAdapterDecoder.NAME, new MqttAdapterDecoder());
                        ch.pipeline().addLast("mqtt-decoder", new MqttDecoder(maxBytesInMessage));
                        //
                        ch.pipeline().addLast(MqttAdapterEncoder.NAME, MqttAdapterEncoder.INSTANCE);
                        //
                        ch.pipeline().addLast(CombineAdapterHandler.NAME, new CombineAdapterHandler());
                        ch.pipeline().addLast(AdapterHandler.NAME, new AdapterHandler());
                    }
                });
        this.scheduledFuture = eventLoopGroup.scheduleWithFixedDelay(new HeartbeatTask(), 30, 60, TimeUnit.SECONDS);
    }

    public AdapterChannel getAdapterChannel(InetSocketAddress broker) {
        return new AdapterChannel(this, broker, getChannel(broker));
    }

    public CompletableFuture<Channel> getChannel(InetSocketAddress broker) {
        final int channelKey = signSafeMod(counter.incrementAndGet(), maxNoOfChannels);
        return pool.computeIfAbsent(broker, (x) -> new ConcurrentHashMap<>())
                .computeIfAbsent(channelKey, __-> createChannel(broker, channelKey));
    }

    private CompletableFuture<Channel> createChannel(InetSocketAddress host, int channelKey) {
        CompletableFuture<Channel> channelFuture = ChannelFutures.toCompletableFuture(bootstrap.register())
                .thenCompose(channel -> ChannelFutures.toCompletableFuture(channel.connect(host)))
                .thenApply(channel -> {
                    channel.closeFuture().addListener(v -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[Proxy Adapter] Removing closed channel from pool {}", channel);
                        }
                        cleanupChannel(host, channelKey);
                    });
                    return channel;
                });
        channelFuture.exceptionally(ex -> {
            log.error("[Proxy Adapter] Connect to : {} failed.", host, ex);
            return null;
        });
        return channelFuture;
    }

    private void cleanupChannel(InetSocketAddress broker, int channelKey) {
        Map<Integer, CompletableFuture<Channel>> brokerChannels = pool.get(broker);
        if (brokerChannels != null) {
            brokerChannels.remove(channelKey);
        }
    }

    public void shutdown() {
        try {
            closeChannels();
            this.scheduledFuture.cancel(true);
            this.eventLoopGroup.shutdownGracefully();
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception", e);
        }
    }

    private void closeChannels() {
        pool.values().forEach(map -> map.values().forEach(future ->
                future.whenComplete((channel, ex) -> {
                    if (channel != null) {
                        channel.close();
                    }
                }))
        );
    }

    private class HeartbeatTask implements Runnable {

        @Override
        public void run() {
            pool.values().forEach(p -> p.values().forEach(f -> {
                f.thenAccept(c -> {
                    if (c.isActive()) {
                        c.writeAndFlush(MqttMessageUtils.pingReq());
                    }
                });
            }));
        }
    }

    public class AdapterHandler extends ChannelInboundHandlerAdapter {

        public static final String NAME = "adapter-handler";

        private final Set<Connection> callbackConnections = Collections.newSetFromMap(new ConcurrentHashMap<>());

        public void registerAdapterChannelInactiveListener(Connection connection) {
            callbackConnections.add(connection);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            callbackConnections.forEach(connection -> connection.getChannel().close());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
            checkArgument(message instanceof MqttAdapterMessage);
            MqttAdapterMessage adapterMsg = (MqttAdapterMessage) message;
            adapterMsg.setEncodeType(MqttAdapterMessage.EncodeType.MQTT_MESSAGE);
            String clientId = adapterMsg.getClientId();
            MqttMessage msg = adapterMsg.getMqttMessage();
            Connection connection = proxyService.getConnectionManager().getConnection(clientId);
            if (connection == null) {
                log.warn("Not find matched connection : {}, adapterMsg : {}", clientId, adapterMsg);
                return;
            }
            MQTTProxyProtocolMethodProcessor processor =
                    ((MQTTProxyProtocolMethodProcessor) connection.getProcessor());
            Channel clientChannel = processor.getChannel();
            try {
                checkState(msg);
                MqttMessageType messageType = adapterMsg.getMqttMessage().fixedHeader().messageType();
                if (log.isDebugEnabled()) {
                    log.debug("AdapterHandler read messageType : {}", messageType);
                }
                switch (messageType) {
                    case DISCONNECT:
                        if (MqttUtils.isNotMqtt3(connection.getProtocolVersion())) {
                            clientChannel.writeAndFlush(adapterMsg);
                        }
                        // When the adapter receives DISCONNECT, we don't need to trigger send disconnect to broker.
                        processor.isDisconnected().set(true);
                        processor.getChannel().close();
                        break;
                    case PUBLISH:
                        MqttPublishMessage pubMessage = (MqttPublishMessage) msg;
                        int packetId = pubMessage.variableHeader().packetId();
                        String topicName = pubMessage.variableHeader().topicName();
                        processor.getPacketIdTopic().put(packetId, topicName);
                        clientChannel.writeAndFlush(adapterMsg);
                        break;
                    case CONNACK:
                        break;
                    case SUBACK:
                        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) msg;
                        int subMessageId = subAckMessage.variableHeader().messageId();
                        if (processor.getSubscribeAckTracker().checkIfSendAck(subMessageId)) {
                            clientChannel.writeAndFlush(adapterMsg);
                        }
                        break;
                    case UNSUBACK:
                        MqttUnsubAckMessage unSubAckMessage = (MqttUnsubAckMessage) msg;
                        int unSubMessageId = unSubAckMessage.variableHeader().messageId();
                        if (processor.getUnsubscribeAckTracker().checkIfSendAck(unSubMessageId)) {
                            clientChannel.writeAndFlush(adapterMsg);
                        }
                        break;
                    case PUBACK:
                        MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) msg;
                        MqttMessageIdVariableHeader variableHeader = pubAckMessage.variableHeader();
                        if (variableHeader instanceof MqttPubReplyMessageVariableHeader) {
                            MqttPubReplyMessageVariableHeader header =
                                    (MqttPubReplyMessageVariableHeader) variableHeader;
                            byte reasonCode = header.reasonCode();
                            if (Mqtt5PubReasonCode.UNSPECIFIED_ERROR.byteValue() == reasonCode) {
                                String sourceTopicName = null;
                                MqttProperties.UserProperties property =
                                        (MqttProperties.UserProperties) header.properties()
                                                .getProperty(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
                                if (property != null) {
                                    List<MqttProperties.StringPair> pairs = property.value();
                                    sourceTopicName = pairs.stream().filter(p -> p.key.equals("topicName"))
                                            .map(p -> p.value).findFirst().orElse(null);
                                }
                                processor.removeTopicBroker(sourceTopicName);
                            }
                        }
                        clientChannel.writeAndFlush(adapterMsg);
                        break;
                    default:
                        clientChannel.writeAndFlush(adapterMsg);
                        break;
                }
            } catch (Throwable ex) {
                log.error("Exception was caught while processing MQTT broker message", ex);
                ctx.close();
                clientChannel.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("exception caught when connect with MoP broker.", cause);
            ctx.close();
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            if (log.isDebugEnabled()) {
                log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
            }
        }
    }
}
