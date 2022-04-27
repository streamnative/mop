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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.TopicFilter;
import io.streamnative.pulsar.handlers.mqtt.adapter.AdapterChannel;
import io.streamnative.pulsar.handlers.mqtt.adapter.MQTTProxyAdapter;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttPubAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttSubAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.properties.PulsarProperties;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ServerRestrictions;
import io.streamnative.pulsar.handlers.mqtt.support.AbstractCommonProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarTopicChangeListener;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.ConnectEvent;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.SystemEventService;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
/**
 * Proxy inbound handler is the bridge between proxy and MoP.
 */
@Slf4j
public class MQTTProxyProtocolMethodProcessor extends AbstractCommonProtocolMethodProcessor {

    @Getter
    private Connection connection;
    private final LookupHandler lookupHandler;
    private final MQTTProxyConfiguration proxyConfig;
    private final PulsarService pulsarService;
    private final Map<String, CompletableFuture<AdapterChannel>> topicBrokers;
    private final Map<InetSocketAddress, AdapterChannel> adapterChannels;
    @Getter
    private final Map<Integer, String> packetIdTopic;
    // Map sequence Id -> topic count
    private final ConcurrentHashMap<Integer, AtomicInteger> subscribeTopicsCount;
    private final MQTTConnectionManager connectionManager;
    private final SystemEventService eventService;
    private final PulsarEventCenter pulsarEventCenter;
    private final MQTTProxyAdapter proxyAdapter;
    private final AtomicBoolean isDisconnected = new AtomicBoolean(false);

    private int pendingSendRequest = 0;
    private final int maxPendingSendRequest;
    private final int resumeReadThreshold;

    public MQTTProxyProtocolMethodProcessor(MQTTProxyService proxyService, ChannelHandlerContext ctx) {
        super(proxyService.getAuthenticationService(),
                proxyService.getProxyConfig().isMqttAuthenticationEnabled(), ctx);
        this.pulsarService = proxyService.getPulsarService();
        this.lookupHandler = proxyService.getLookupHandler();
        this.proxyConfig = proxyService.getProxyConfig();
        this.connectionManager = proxyService.getConnectionManager();
        this.eventService = proxyService.getEventService();
        this.topicBrokers = new ConcurrentHashMap<>();
        this.adapterChannels = new ConcurrentHashMap<>();
        this.subscribeTopicsCount = new ConcurrentHashMap<>();
        this.packetIdTopic = new ConcurrentHashMap<>();
        this.pulsarEventCenter = proxyService.getEventCenter();
        this.proxyAdapter = proxyService.getProxyAdapter();
        this.maxPendingSendRequest = proxyConfig.getMaxPendingSendRequest();
        this.resumeReadThreshold = maxPendingSendRequest / 2;
    }

    @Override
    public void doProcessConnect(MqttAdapterMessage adapter, String userRole, ClientRestrictions clientRestrictions) {
        final MqttConnectMessage msg = (MqttConnectMessage) adapter.getMqttMessage();
        final ServerRestrictions serverRestrictions = ServerRestrictions.builder()
                .receiveMaximum(proxyConfig.getReceiveMaximum())
                .build();
        connection = Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(msg.payload().clientIdentifier())
                .userRole(userRole)
                .connectMessage(msg)
                .clientRestrictions(clientRestrictions)
                .serverRestrictions(serverRestrictions)
                .channel(channel)
                .connectionManager(connectionManager)
                .processor(this)
                .eventCenter(pulsarEventCenter)
                .build();
        connection.sendConnAck();
        ConnectEvent connectEvent = ConnectEvent.builder()
                .clientId(connection.getClientId())
                .address(pulsarService.getAdvertisedAddress())
                .build();
        eventService.sendConnectEvent(connectEvent);
    }

    @Override
    public void processPublish(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] publish to topic = {}, CId={}",
                    msg.variableHeader().topicName(), connection.getClientId());
        }
        final int packetId = msg.variableHeader().packetId();
        final String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
        adapter.setClientId(connection.getClientId());
        startPublish()
                .thenCompose(__ ->  writeToBroker(pulsarTopicName, adapter))
                .whenComplete((unused, ex) -> {
                    endPublish();
                    if (ex != null) {
                        msg.release();
                        Throwable cause = ex.getCause();
                        log.error("[Proxy Publish] Failed to publish for topic : {}, CId : {}",
                                msg.variableHeader().topicName(), connection.getClientId(), cause);
                        MqttAck pubAck = MqttPubAck.errorBuilder(connection.getProtocolVersion())
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR)
                                .reasonString(String.format("Failed to publish for topic, because of look up error %s",
                                        cause.getMessage()))
                                .build();
                        connection.sendAckThenClose(pubAck)
                                .thenAccept(__ -> connection.decrementServerReceivePubMessage());
                    }
                });
    }

    private CompletableFuture<Void> startPublish() {
        if (++pendingSendRequest == maxPendingSendRequest) {
            ctx.channel().config().setAutoRead(false);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> endPublish() {
        if (--pendingSendRequest == resumeReadThreshold) {
            if (!ctx.channel().config().isAutoRead()) {
                ctx.channel().config().setAutoRead(true);
                ctx.read();
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void processPubAck(MqttAdapterMessage adapter) {
        final MqttPubAckMessage msg = (MqttPubAckMessage) adapter.getMqttMessage();
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}]", NettyUtils.getConnection(channel).getClientId());
        }
        final int packetId = msg.variableHeader().messageId();
        adapter.setClientId(connection.getClientId());
        String topicName = packetIdTopic.remove(packetId);
        if (topicName != null) {
            final String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(topicName,
                    proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                    TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
            writeToBroker(pulsarTopicName, adapter)
                    .exceptionally(ex -> {
                        log.error("[Proxy Publish] Failed write pub ack {} to topic {} CId : {}",
                                msg, topicName, connection.getClientId(), ex);
                        return null;
                    });
        } else {
            log.warn("[Proxy Publish] Failed to get topic name by packet id {} while process pub ack {} CId : {}",
                   packetId, msg, connection.getClientId());
        }
    }

    @Override
    public void processPingReq(final MqttAdapterMessage msg) {
        String clientId = connection.getClientId();
        topicBrokers.values().forEach(adapterChannel -> {
            adapterChannel.thenAccept(channel -> {
                msg.setClientId(clientId);
                channel.writeAndFlush(msg);
            });
        });
    }

    @Override
    public void processDisconnect(final MqttAdapterMessage msg) {
        if (isDisconnected.compareAndSet(false, true)) {
            String clientId = connection.getClientId();
            if (log.isDebugEnabled()) {
                log.debug("[Proxy Disconnect] [{}] ", clientId);
            }
            msg.setClientId(clientId);
            // Deduplicate the channel to avoid sending disconnects many times.
            CompletableFuture.allOf(topicBrokers.values().toArray(new CompletableFuture[0]))
                    .whenComplete((result, ex) -> topicBrokers.values().stream()
                            .filter(future -> !future.isCompletedExceptionally())
                            .map(CompletableFuture::join)
                            .collect(Collectors.toSet())
                            .forEach(channel -> channel.writeAndFlush(msg)));
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Disconnect is already triggered, ignore");
            }
        }
    }

    @Override
    public void processConnectionLost() {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Connection Lost] [{}] ", connection.getClientId());
        }
        if (connection != null) {
            // If client close the channel without calling disconnect, then we should call disconnect to notify broker
            // to clean up the resource.
            processDisconnect(new MqttAdapterMessage(MqttMessageUtils.createMqttDisconnectMessage()));
            connectionManager.removeConnection(connection);
            connection.close();
        }
        topicBrokers.clear();
    }

    @Override
    public void processSubscribe(final MqttAdapterMessage adapter) {
        final MqttSubscribeMessage msg = (MqttSubscribeMessage) adapter.getMqttMessage();
        final String clientId = connection.getClientId();
        final int packetId = msg.variableHeader().messageId();
        adapter.setClientId(clientId);
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Subscribe] [{}] msg: {}", clientId, msg);
        }
        doSubscribe(adapter, true)
                .thenAccept(__ -> registerTopicListener(adapter))
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.error("[Proxy Subscribe] Failed to process subscribe for {}", clientId, realCause);
                    MqttAck subAck = MqttSubAck.errorBuilder(connection.getProtocolVersion())
                            .packetId(packetId)
                            .errorReason(MqttSubAck.ErrorReason.UNSPECIFIED_ERROR)
                            .reasonString("[ MOP ERROR ]" + realCause.getMessage())
                            .build();
                    connection.sendAckThenClose(subAck);
                    subscribeTopicsCount.remove(packetId);
                    return null;
                });
    }

    private void registerTopicListener(final MqttAdapterMessage adapter) {
        final MqttSubscribeMessage msg = (MqttSubscribeMessage) adapter.getMqttMessage();
        for (MqttTopicSubscription subscription : msg.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicName();
            if (MqttUtils.isRegexFilter(topicFilter)) {
                connection.addTopicChangeListener(new PulsarTopicChangeListener() {
                    @Override
                    public void onTopicLoad(TopicName changedTopicName) {
                        String subTopicName = subscription.topicName();
                        String pulsarTopicNameStr = PulsarTopicUtils.getPulsarTopicName(subTopicName,
                                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                                true, TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
                        TopicName pulsarTopicName = TopicName.get(pulsarTopicNameStr);
                        // Support user use namespace level regex
                        if (!Objects.equals(changedTopicName.getNamespace(), pulsarTopicName.getNamespace())) {
                            return;
                        }
                        TopicFilter topicFilter = PulsarTopicUtils.getTopicFilter(subTopicName);
                        String decodedTopicName = Codec.decode(changedTopicName.getLocalName());
                        if (!topicFilter.test(decodedTopicName)) {
                            return;
                        }
                        MqttProperties.UserProperty property = new MqttProperties.UserProperty(
                                PulsarProperties.InitialPosition.name(),
                                        String.valueOf(CommandSubscribe.InitialPosition.Earliest_VALUE));
                        MqttProperties mqttProperties = new MqttProperties();
                        mqttProperties.add(property);
                        MqttSubscribeMessage message = MqttMessageBuilders
                                .subscribe()
                                .messageId(msg.variableHeader().messageId())
                                .addSubscription(subscription.qualityOfService(), decodedTopicName)
                                .properties(mqttProperties)
                                .build();
                        MqttAdapterMessage adapterMessage = new MqttAdapterMessage(connection.getClientId(), message);
                        doSubscribe(adapterMessage, false)
                                .thenRun(() -> log.info("[{}] Subscribe new topic [{}] success",
                                        connection.getClientId(), Codec.decode(changedTopicName.toString())))
                                .exceptionally(ex -> {
                                    log.error("[{}][{}] Subscribe new topic [{}] Fail, the message is {}",
                                            connection.getClientId(), topicFilter, changedTopicName, ex.getMessage());
                                    return null;
                                });
                    }

                    @Override
                    public void onTopicUnload(TopicName topicName) {
                        // NO-OP
                    }
                });
            }
        }
    }

    private CompletableFuture<Void> doSubscribe(final MqttAdapterMessage adapter, final boolean incrementCounter) {
        final MqttSubscribeMessage message = (MqttSubscribeMessage) adapter.getMqttMessage();
        return PulsarTopicUtils.asyncGetTopicsForSubscribeMsg(message, proxyConfig.getDefaultTenant(),
                        proxyConfig.getDefaultNamespace(),
                        pulsarService, proxyConfig.getDefaultTopicDomain())
                .thenCompose(topics -> {
                    int packetId = message.variableHeader().messageId();
                    if (CollectionUtils.isEmpty(topics)) {
                        MqttAck subAck = MqttSubAck.successBuilder(connection.getProtocolVersion())
                                .packetId(packetId)
                                .grantedQos(new ArrayList<>(message.payload().topicSubscriptions().stream()
                                        .map(MqttTopicSubscription::qualityOfService)
                                        .collect(Collectors.toSet())))
                                .build();
                        connection.sendAck(subAck);
                        return CompletableFuture.completedFuture(null);
                    }
                    List<CompletableFuture<Void>> subscribeFutures = topics.stream()
                            .map(topic -> {
                                CompletableFuture<Void> future = writeToBroker(topic, adapter);
                                if (incrementCounter) {
                                    future.thenAccept(__ ->
                                            increaseSubscribeTopicsCount(packetId, 1));
                                }
                                future.thenAccept(__ -> registerAdapterChannelInactiveListener(topic));
                                return future;
                            }).collect(Collectors.toList());
                    return FutureUtil.waitForAll(subscribeFutures);
                });
    }

    private void registerAdapterChannelInactiveListener(final String topic) {
        CompletableFuture<AdapterChannel> adapterChannel = topicBrokers.get(topic);
        adapterChannel.thenAccept(channel -> channel.registerAdapterChannelInactiveListener(connection));
    }

    @Override
    public void processUnSubscribe(final MqttAdapterMessage adapter) {
        final MqttUnsubscribeMessage msg = (MqttUnsubscribeMessage) adapter.getMqttMessage();
        if (log.isDebugEnabled()) {
            log.debug("[Proxy UnSubscribe] [{}]", connection.getClientId());
        }
        List<String> topics = msg.payload().topics();
        List<CompletableFuture<Void>> futures = new ArrayList<>(topics.size());
        for (String topic : topics) {
            futures.add(writeToBroker(topic, adapter));
        }
        FutureUtil.waitForAll(Collections.unmodifiableList(futures))
                .exceptionally(ex -> {
                    log.error("[Proxy UnSubscribe] Failed to perform lookup request", ex);
                    channel.close();
                    return null;
        });
    }

    private CompletableFuture<Void> writeToBroker(final String topic, final MqttAdapterMessage msg) {
        CompletableFuture<AdapterChannel> proxyExchanger = connectToBroker(topic);
        return proxyExchanger.thenCompose(exchanger -> exchanger.writeAndFlush(msg));
    }

    private CompletableFuture<AdapterChannel> connectToBroker(final String topic) {
        return topicBrokers.computeIfAbsent(topic,
                key -> lookupHandler.findBroker(TopicName.get(topic)).thenApply(mqttBroker -> {
                    return adapterChannels.computeIfAbsent(mqttBroker, key1 -> {
                        AdapterChannel adapterChannel = proxyAdapter.getAdapterChannel(mqttBroker);
                        adapterChannel.writeAndFlush(new MqttAdapterMessage(
                                connection.getClientId(),
                                connection.getConnectMessage()));
                        return adapterChannel;
                    });
                }));
    }

    public Channel getChannel() {
        return this.channel;
    }

    /**
     *  MQTT support subscribe many topics in one subscribe request.
     *  We need to record it's subscribe count.
     * @param seq
     * @param count
     */
    private void increaseSubscribeTopicsCount(int seq, int count) {
        AtomicInteger subscribeCount = subscribeTopicsCount.putIfAbsent(seq, new AtomicInteger(count));
        if (subscribeCount != null) {
            subscribeCount.addAndGet(count);
        }
    }

    private int decreaseSubscribeTopicsCount(int seq) {
        AtomicInteger subscribeCount = subscribeTopicsCount.get(seq);
        if (subscribeCount == null) {
            log.warn("Unexpected subscribe behavior for the proxy, respond seq {} "
                    + "but but the seq does not tracked by the proxy. ", seq);
            return -1;
        } else {
            int value = subscribeCount.decrementAndGet();
            if (value == 0) {
                subscribeTopicsCount.remove(seq);
            }
            return value;
        }
    }

    /**
     * If one sub-ack succeed, we need to decrease it's sub-count.
     * As the sub-count return zero, it means the subscribe action succeed.
     * @param packetId
     * @return
     */
    public boolean checkIfSendSubAck(int packetId) {
        // In regex subscription, we don't need to send any ack to client.
        AtomicInteger counter = subscribeTopicsCount.get(packetId);
        if (counter == null || 0 == counter.get()) {
            return false;
        }
        return decreaseSubscribeTopicsCount(packetId) == 0;
    }

    public AtomicBoolean isDisconnected() {
        return this.isDisconnected;
    }
}
