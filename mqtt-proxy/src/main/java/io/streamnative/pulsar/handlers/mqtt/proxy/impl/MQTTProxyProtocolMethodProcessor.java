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
package io.streamnative.pulsar.handlers.mqtt.proxy.impl;

import static io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils.createMqtt5ConnectMessage;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils.createMqttPublishMessage;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils.createMqttSubscribeMessage;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.common.AbstractCommonProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.common.TopicFilter;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.common.event.AutoSubscribeHandler;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttPubAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttSubAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.common.messages.properties.PulsarProperties;
import io.streamnative.pulsar.handlers.mqtt.common.mqtt5.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.common.mqtt5.restrictions.ServerRestrictions;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.ConnectEvent;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.SystemEventService;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.common.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.common.utils.PulsarTopicUtils;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.proxy.channel.AdapterChannel;
import io.streamnative.pulsar.handlers.mqtt.proxy.channel.MQTTProxyAdapter;
import io.streamnative.pulsar.handlers.mqtt.proxy.handler.LookupHandler;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
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

    private final PulsarService pulsarService;

    @Getter
    private Connection connection;
    private final LookupHandler lookupHandler;
    private final MQTTProxyConfiguration proxyConfig;
    @Getter
    private final Map<String, CompletableFuture<AdapterChannel>> topicBrokers;
    private final Map<InetSocketAddress, AdapterChannel> adapterChannels;
    @Getter
    private final Map<Integer, String> packetIdTopic;
    @Getter
    private final MessageAckTracker subscribeAckTracker;
    @Getter
    private final MessageAckTracker unsubscribeAckTracker;
    private final MQTTConnectionManager connectionManager;
    private final SystemEventService eventService;
    private final MQTTProxyAdapter proxyAdapter;

    private final AtomicBoolean isDisconnected = new AtomicBoolean(false);
    private final AutoSubscribeHandler autoSubscribeHandler;

    private int pendingSendRequest = 0;
    private final int maxPendingSendRequest;
    private final int resumeReadThreshold;

    public MQTTProxyProtocolMethodProcessor(MQTTProxyService proxyService, ChannelHandlerContext ctx) {
        super(proxyService.getAuthenticationService(),
                proxyService.getProxyConfig().isMqttAuthenticationEnabled(),
                ctx);
        pulsarService = proxyService.getPulsarService();
        this.lookupHandler = proxyService.getLookupHandler();
        this.proxyConfig = proxyService.getProxyConfig();
        this.connectionManager = proxyService.getConnectionManager();
        this.eventService = proxyService.getEventService();
        this.topicBrokers = new ConcurrentHashMap<>();
        this.adapterChannels = new ConcurrentHashMap<>();
        this.subscribeAckTracker = new MessageAckTracker();
        this.unsubscribeAckTracker = new MessageAckTracker();
        this.packetIdTopic = new ConcurrentHashMap<>();
        this.proxyAdapter = proxyService.getProxyAdapter();
        this.maxPendingSendRequest = proxyConfig.getMaxPendingSendRequest();
        this.resumeReadThreshold = maxPendingSendRequest / 2;
        this.autoSubscribeHandler = new AutoSubscribeHandler(proxyService.getEventCenter());
    }

    @Override
    public void doProcessConnect(MqttAdapterMessage adapter, String userRole,
                                 AuthenticationDataSource authData, ClientRestrictions clientRestrictions) {
        MqttConnectMessage msg = (MqttConnectMessage) adapter.getMqttMessage();
        final ServerRestrictions serverRestrictions = ServerRestrictions.builder()
                .receiveMaximum(proxyConfig.getReceiveMaximum())
                .maximumPacketSize(proxyConfig.getMqttMessageMaxLength())
                .build();
        connection = Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(msg.payload().clientIdentifier())
                .userRole(userRole)
                .connectMessage(msg)
                .clientRestrictions(clientRestrictions)
                .serverRestrictions(serverRestrictions)
                .channel(channel)
                .authData(authData)
                .connectionManager(connectionManager)
                .processor(this)
                .build();
        connection.sendConnAck();

        if (proxyConfig.isMqttAuthorizationEnabled()) {
            MqttConnectMessage connectMessage = createMqtt5ConnectMessage(msg);
            msg = connectMessage;
            connection.setConnectMessage(msg);
        }

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
        if (proxyConfig.isMqttAuthorizationEnabled()) {
            MqttPublishMessage mqttMessage = createMqttPublishMessage(msg, connection.getUserRole());
            adapter.setMqttMessage(mqttMessage);
        }
        startPublish()
                .thenCompose(__ ->  writeToBroker(pulsarTopicName, adapter))
                .whenComplete((unused, ex) -> {
                    endPublish();
                    if (ex != null) {
                        msg.release();
                        Throwable cause = ex.getCause();
                        log.error("[Proxy Publish] Failed to publish for topic : {}, CId : {}",
                                msg.variableHeader().topicName(), connection.getClientId(), cause);
                        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder =
                                MqttPubAck.errorBuilder(connection.getProtocolVersion())
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR);
                        if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                            pubAckBuilder.reasonString(
                                            String.format("Failed to publish for topic, because of look up error %s",
                                            cause.getMessage()));
                        }
                        connection.sendAckThenClose(pubAckBuilder.build())
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
                channel.writeAndFlush(connection, msg);
            });
        });
    }

    @Override
    public boolean connectionEstablished() {
        return connection != null && connection.getState() == Connection.ConnectionState.ESTABLISHED;
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
                            .forEach(channel -> channel.writeAndFlush(connection, msg)));
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Disconnect is already triggered, ignore");
            }
        }
    }

    @Override
    public void processConnectionLost() {
        if (log.isDebugEnabled()) {
            String clientId = "unknown";
            if (connection != null) {
                clientId = connection.getClientId();
            }
            log.debug("[Proxy Connection Lost] [{}] ", clientId);
        }
        autoSubscribeHandler.close();
        if (connection != null) {
            // If client close the channel without calling disconnect, then we should call disconnect to notify broker
            // to clean up the resource.
            processDisconnect(new MqttAdapterMessage(MqttMessageUtils.createMqttDisconnectMessage()));
            connectionManager.removeConnection(connection);
            connection.cleanup();
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
        registerTopicListener(adapter);
        if (proxyConfig.isMqttAuthorizationEnabled()) {
            MqttSubscribeMessage mqttMessage = createMqttSubscribeMessage(msg, connection.getUserRole());
            adapter.setMqttMessage(mqttMessage);
        }
        doSubscribe(adapter, false)
                .exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    log.error("[Proxy Subscribe] Failed to process subscribe for {}", clientId, realCause);
                    MqttSubAck.MqttSubErrorAckBuilder subAckBuilder =
                            MqttSubAck.errorBuilder(connection.getProtocolVersion())
                            .packetId(packetId)
                            .errorReason(MqttSubAck.ErrorReason.UNSPECIFIED_ERROR);
                    if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                        subAckBuilder.reasonString("[ MOP ERROR ]" + realCause.getMessage());
                    }
                    connection.sendAckThenClose(subAckBuilder.build());
                    subscribeAckTracker.remove(packetId);
                    return null;
                });
    }

    private void registerTopicListener(final MqttAdapterMessage adapter) {
        final MqttSubscribeMessage msg = (MqttSubscribeMessage) adapter.getMqttMessage();
        for (MqttTopicSubscription subscription : msg.payload().topicSubscriptions()) {
            String topicFilter = subscription.topicName();
            if (MqttUtils.isRegexFilter(topicFilter)) {
                autoSubscribeHandler.register(
                        PulsarTopicUtils.getTopicFilter(topicFilter),
                        (encodedChangedTopic) -> {
                            String mqttTopicName = getMqttTopicName(subscription,
                                    encodedChangedTopic);
                            MqttProperties mqttProperties = new MqttProperties();
                            mqttProperties.add(new MqttProperties.UserProperty(
                                    PulsarProperties.InitialPosition.name(),
                                    String.valueOf(CommandSubscribe.InitialPosition.Earliest_VALUE)));
                            MqttSubscribeMessage message = MqttMessageBuilders
                                    .subscribe()
                                    .messageId(msg.variableHeader().messageId())
                                    .addSubscription(subscription.qualityOfService(), mqttTopicName)
                                    .properties(mqttProperties)
                                    .build();
                            MqttAdapterMessage adapterMessage =
                                    new MqttAdapterMessage(connection.getClientId(), message);
                            doSubscribe(adapterMessage, true)
                                    .thenRun(() -> log.info("[{}] Subscribe new topic [{}] success",
                                            connection.getClientId(), mqttTopicName))
                                    .exceptionally(ex -> {
                                        log.error("[{}][{}] Subscribe new topic [{}] Fail, the message is {}",
                                                connection.getClientId(), topicFilter, mqttTopicName,
                                                ex.getMessage());
                                        return null;
                                    });
                        });
            }
        }
    }

    private CompletableFuture<Void> doSubscribe(final MqttAdapterMessage adapter, final boolean isAutoSubscribe) {
        final MqttSubscribeMessage message = (MqttSubscribeMessage) adapter.getMqttMessage();
        final int packetId = message.variableHeader().messageId();
        List<CompletableFuture<Void>> futures = message.payload().topicSubscriptions().stream()
                .map(subscription -> PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(subscription.topicName(),
                                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(), pulsarService,
                                proxyConfig.getDefaultTopicDomain())
                        .thenCompose(pulsarTopicNames -> {
                            if (CollectionUtils.isEmpty(pulsarTopicNames)) {
                                MqttAck subAck = MqttSubAck.successBuilder(connection.getProtocolVersion())
                                        .packetId(packetId)
                                        .grantedQos(new ArrayList<>(message.payload().topicSubscriptions().stream()
                                                .map(MqttTopicSubscription::qualityOfService)
                                                .collect(Collectors.toSet())))
                                        .build();
                                connection.sendAck(subAck);
                                return CompletableFuture.completedFuture(null);
                            }
                            if (!isAutoSubscribe) {
                                subscribeAckTracker.increment(packetId, pulsarTopicNames.size());
                            }
                            List<CompletableFuture<Void>> writeFutures = pulsarTopicNames.stream()
                                    .map(encodedPulsarTopicName -> {
                                        String mqttTopicName = getMqttTopicName(subscription,
                                                encodedPulsarTopicName);
                                        MqttSubscribeMessage subscribeMessage = MqttMessageBuilders.subscribe()
                                                .messageId(message.variableHeader().messageId())
                                                .addSubscription(subscription.qualityOfService(), mqttTopicName)
                                                .properties(message.idAndPropertiesVariableHeader().properties())
                                                .build();
                                        MqttAdapterMessage mqttAdapterMessage =
                                                new MqttAdapterMessage(connection.getClientId(), subscribeMessage);
                                        return writeToBroker(encodedPulsarTopicName, mqttAdapterMessage);
                                    }).collect(Collectors.toList());
                            return FutureUtil.waitForAll(writeFutures);
                        })
                ).collect(Collectors.toList());
        return FutureUtil.waitForAll(futures);
    }

    private String getMqttTopicName(MqttTopicSubscription subscription, String encodedPulsarTopicName) {
        TopicName encodedPulsarTopicNameObj = TopicName.get(encodedPulsarTopicName);
        boolean isDefaultPulsarEncodedTopicName = PulsarTopicUtils.isDefaultDomainAndNs(
                encodedPulsarTopicNameObj, proxyConfig.getDefaultTopicDomain(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace());
        if (isDefaultPulsarEncodedTopicName) {
            return MqttUtils.isRegexFilter(subscription.topicName())
                    ? Codec.decode(encodedPulsarTopicNameObj.getLocalName())
                    : subscription.topicName();
        } else {
            return Codec.decode(encodedPulsarTopicName);
        }
    }

    @Override
    public void processUnSubscribe(final MqttAdapterMessage adapter) {
        final MqttUnsubscribeMessage msg = (MqttUnsubscribeMessage) adapter.getMqttMessage();
        if (log.isDebugEnabled()) {
            log.debug("[Proxy UnSubscribe] [{}]", connection.getClientId());
        }
        msg.payload().topics().stream().map(topic -> {
            if (MqttUtils.isRegexFilter(topic)) {
                TopicFilter filter = PulsarTopicUtils.getTopicFilter(topic);
                autoSubscribeHandler.unregister(filter);
            }
            return PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(topic, proxyConfig.getDefaultTenant(),
                    proxyConfig.getDefaultNamespace(), pulsarService, proxyConfig.getDefaultTopicDomain());
        }).reduce((pre, curr) -> pre.thenCombine(curr, (l, r) -> {
            List<String> container = Lists.newArrayListWithCapacity(l.size() + r.size());
            container.addAll(l);
            container.addAll(r);
            return container;
        })).orElseGet(() -> CompletableFuture.completedFuture(Collections.emptyList()))
        .thenCompose(pulsarTopics -> {
            MqttUnsubscribeMessage mqttMessage = (MqttUnsubscribeMessage) adapter.getMqttMessage();
            unsubscribeAckTracker.increment(mqttMessage.variableHeader().messageId(), pulsarTopics.size());
            List<CompletableFuture<Void>> writeFutures = pulsarTopics.stream().map(pulsarTopic -> {
                MqttUnsubscribeMessage unsubscribeMessage = MqttMessageBuilders
                        .unsubscribe()
                        .messageId(mqttMessage.variableHeader().messageId())
                        .addTopicFilter(Codec.decode(pulsarTopic))
                        .build();
                MqttAdapterMessage mqttAdapterMessage =
                        new MqttAdapterMessage(connection.getClientId(), unsubscribeMessage);
                return writeToBroker(pulsarTopic, mqttAdapterMessage);
            }).collect(Collectors.toList());
            return FutureUtil.waitForAll(writeFutures);
        }).exceptionally(ex -> {
            log.error("[Proxy UnSubscribe] Failed to perform lookup request", ex);
            unsubscribeAckTracker.remove(msg.variableHeader().messageId());
            channel.close();
            return null;
        });
    }

    private CompletableFuture<Void> writeToBroker(final String topic, final MqttAdapterMessage msg) {
        CompletableFuture<AdapterChannel> proxyExchanger = connectToBroker(topic);
        return proxyExchanger.thenCompose(exchanger -> exchanger.writeAndFlush(connection, msg));
    }

    private CompletableFuture<AdapterChannel> connectToBroker(final String topic) {
        return topicBrokers.computeIfAbsent(topic,
                key -> lookupHandler.findBroker(TopicName.get(topic)).thenApply(mqttBroker ->
                        adapterChannels.computeIfAbsent(mqttBroker, key1 -> {
                            AdapterChannel adapterChannel = proxyAdapter.getAdapterChannel(mqttBroker);
                            final MqttConnectMessage connectMessage = connection.getConnectMessage();
                            adapterChannel.writeAndFlush(connection, new MqttAdapterMessage(connection.getClientId(),
                                    connectMessage));
                            adapterChannel.registerClosureListener(future -> {
                                topicBrokers.values().remove(adapterChannel);
                                if (topicBrokers.values().size() <= 1) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Adapter channel inactive, close related connection {}", connection);
                                    }
                                    connection.getChannel().close();
                                } else {
                                    if (log.isDebugEnabled()) {
                                        log.debug("connection {} has more than one AdapterChannel", connection);
                                    }
                                }
                            });
                            return adapterChannel;
                        })
                )
        );
    }

    public void removeTopicBroker(String topic) {
        if (StringUtils.isNotEmpty(topic)) {
            String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(topic,
                    proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                    TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
            final CompletableFuture<AdapterChannel> brokerChannel = topicBrokers.remove(pulsarTopicName);
            if (brokerChannel != null) {
                brokerChannel.thenAccept(channel -> {
                    if (log.isDebugEnabled()) {
                        log.debug("remove topic : {} from broker : {}", topic, channel.getBroker());
                    }
                });
            }
        }
    }

    public AtomicBoolean isDisconnected() {
        return this.isDisconnected;
    }
}
