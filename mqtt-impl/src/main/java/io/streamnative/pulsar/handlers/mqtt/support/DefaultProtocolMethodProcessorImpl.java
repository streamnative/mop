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
package io.streamnative.pulsar.handlers.mqtt.support;

import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createWillMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.pingResp;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.topicSubscriptions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.MQTTSubscriptionManager;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;
import io.streamnative.pulsar.handlers.mqtt.TopicFilter;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.restrictions.InvalidSessionExpireIntervalException;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.DisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.UnsubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.properties.PulsarProperties;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ServerRestrictions;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarTopicChangeListener;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.WillMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Default implementation of protocol method processor.
 */
@Slf4j
public class DefaultProtocolMethodProcessorImpl extends AbstractCommonProtocolMethodProcessor {
    private final PulsarService pulsarService;
    private final QosPublishHandlers qosPublishHandlers;
    private final MQTTServerConfiguration configuration;
    private final MQTTServerCnx serverCnx;
    private final PacketIdGenerator packetIdGenerator;
    private final OutstandingPacketContainer outstandingPacketContainer;
    private final AuthorizationService authorizationService;
    private final MQTTMetricsCollector metricsCollector;
    private final MQTTConnectionManager connectionManager;
    private final MQTTSubscriptionManager mqttSubscriptionManager;
    private final WillMessageHandler willMessageHandler;
    private final RetainedMessageHandler retainedMessageHandler;
    private Connection connection;
    private final PulsarEventCenter eventCenter;
    @Getter
    private final CompletableFuture<Void> inactiveFuture = new CompletableFuture<>();

    public DefaultProtocolMethodProcessorImpl(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService.getAuthenticationService(),
                mqttService.getServerConfiguration().isMqttAuthenticationEnabled(), ctx);
        this.pulsarService = mqttService.getPulsarService();
        this.configuration = mqttService.getServerConfiguration();
        this.qosPublishHandlers = new QosPublishHandlersImpl(mqttService, configuration, ctx.channel());
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.outstandingPacketContainer = new OutstandingPacketContainerImpl();
        this.authorizationService = mqttService.getAuthorizationService();
        this.metricsCollector = mqttService.getMetricsCollector();
        this.connectionManager = mqttService.getConnectionManager();
        this.mqttSubscriptionManager = mqttService.getSubscriptionManager();
        this.willMessageHandler = mqttService.getWillMessageHandler();
        this.retainedMessageHandler = mqttService.getRetainedMessageHandler();
        this.serverCnx = new MQTTServerCnx(pulsarService, ctx);
        this.eventCenter = mqttService.getEventCenter();
    }

    @Override
    public void doProcessConnect(MqttAdapterMessage adapter, String userRole, ClientRestrictions clientRestrictions) {
        final MqttConnectMessage msg = (MqttConnectMessage) adapter.getMqttMessage();
        ServerRestrictions serverRestrictions = ServerRestrictions.builder()
                .receiveMaximum(configuration.getReceiveMaximum())
                .build();
        connection = Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(msg.payload().clientIdentifier())
                .userRole(userRole)
                .willMessage(createWillMessage(msg))
                .clientRestrictions(clientRestrictions)
                .serverRestrictions(serverRestrictions)
                .channel(channel)
                .connectionManager(connectionManager)
                .eventCenter(eventCenter)
                .adapter(adapter.isAdapter())
                .processor(this)
                .build();
        metricsCollector.addClient(NettyUtils.getAndSetAddress(channel));
        connection.sendConnAck();
    }

    @Override
    public void processPubAck(MqttAdapterMessage adapter) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}] msg: {}", connection.getClientId(), adapter);
        }
        final MqttPubAckMessage msg = (MqttPubAckMessage) adapter.getMqttMessage();
        int packetId = msg.variableHeader().messageId();
        OutstandingPacket packet = outstandingPacketContainer.remove(packetId);
        if (packet != null) {
            packet.getConsumer().getSubscription().acknowledgeMessage(
                    Collections.singletonList(PositionImpl.get(packet.getLedgerId(), packet.getEntryId())),
                    CommandAck.AckType.Individual, Collections.emptyMap());
            packet.getConsumer().getPendingAcks().remove(packet.getLedgerId(), packet.getEntryId());
            packet.getConsumer().incrementPermits();
        }
    }

    @Override
    public void processPublish(MqttAdapterMessage adapter) {
        if (log.isDebugEnabled()) {
            log.debug("[Publish] [{}] msg: {}", connection.getClientId(), adapter);
        }
        MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        CompletableFuture<Void> result;
        if (!configuration.isMqttAuthorizationEnabled()) {
            if (log.isDebugEnabled()) {
                log.debug("[Publish] authorization is disabled, allowing client. CId={}, userRole={}",
                        connection.getClientId(), connection.getUserRole());
            }
            result = doPublish(adapter);
        } else {
            result = this.authorizationService.canProduceAsync(TopicName.get(msg.variableHeader().topicName()),
                            connection.getUserRole(), new AuthenticationDataCommand(connection.getUserRole()))
                    .thenCompose(authorized -> authorized ? doPublish(adapter) : doUnauthorized(adapter));
        }
        result.thenAccept(__ -> msg.release())
                .exceptionally(ex -> {
                    Throwable cause = ex.getCause();
                    log.error("[Publish] [{}] Write {} to Pulsar topic failed.",
                            msg.variableHeader().topicName(), msg, cause);
                    msg.release();
                    return null;
                });
    }

    private CompletableFuture<Void> doUnauthorized(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        log.error("[Publish] not authorized to topic={}, userRole={}, CId= {}",
                msg.variableHeader().topicName(), connection.getUserRole(),
                connection.getClientId());
        PublishAck unAuthorizedAck = PublishAck.builder()
                .success(false)
                .packetId(msg.variableHeader().packetId())
                .reasonCode(Mqtt5PubReasonCode.NOT_AUTHORIZED)
                .reasonString("Not Authorized!")
                .build();
        connection.getAckHandler().sendPublishAck(connection, unAuthorizedAck);
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> doPublish(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        metricsCollector.addSend(msg.payload().readableBytes());
        switch (qos) {
            case AT_MOST_ONCE:
                return this.qosPublishHandlers.qos0().publish(adapter);
            case AT_LEAST_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(adapter);
                return this.qosPublishHandlers.qos1().publish(adapter);
            case EXACTLY_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(adapter);
                return this.qosPublishHandlers.qos2().publish(adapter);
            default:
                log.error("[Publish] Unknown QoS-Type:{}", qos);
                connection.getChannel().close();
                return FutureUtil.failedFuture(new IllegalArgumentException("Unknown Qos-Type: " + qos));
        }
    }

    private void checkServerReceivePubMessageAndIncrementCounterIfNeeded(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        // check mqtt 5.0
        if (!MqttUtils.isMqtt5(connection.getProtocolVersion())) {
            return;
        }
        if (connection.getServerReceivePubMessage() >= connection.getClientRestrictions().getReceiveMaximum()) {
            log.warn("Client publish exceed server receive maximum , the receive maximum is {}",
                    connection.getServerRestrictions().getReceiveMaximum());
            PublishAck quotaExceededAck = PublishAck.builder()
                    .success(false)
                    .reasonCode(Mqtt5PubReasonCode.QUOTA_EXCEEDED)
                    .packetId(msg.variableHeader().packetId())
                    .reasonString(String.format("Publish exceed server receive maximum %s.",
                            connection.getServerRestrictions().getReceiveMaximum()))
                    .build();
            connection.getAckHandler().sendPublishAck(connection, quotaExceededAck);
        } else {
            connection.incrementServerReceivePubMessage();
        }
    }

    @Override
    public void processDisconnect(MqttAdapterMessage adapter) {
        // When login, checkState(msg) failed, connection is null.
        final MqttMessage msg = adapter.getMqttMessage();
        if (connection == null) {
            channel.close();
            return;
        }
        final String clientId = connection.getClientId();
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", clientId);
        }
        // If client update session timeout interval property.
        Optional<Integer> newSessionExpireInterval;
        if ((newSessionExpireInterval = MqttPropertyUtils
                .getUpdateSessionExpireIntervalIfExist(connection.getProtocolVersion(), msg)).isPresent()) {
            try {
                connection.updateSessionExpireInterval(newSessionExpireInterval.get());
            } catch (InvalidSessionExpireIntervalException e) {
                DisconnectAck disconnectAck = DisconnectAck
                        .builder()
                        .success(false)
                        .reasonCode(Mqtt5DisConnReasonCode.PROTOCOL_ERROR)
                        .reasonString(String.format("Disconnect with wrong session expire interval value."
                                + " the value is %s", newSessionExpireInterval))
                        .build();
                connection.getAckHandler().sendDisconnectAck(connection, disconnectAck);
            }
        } else {
            DisconnectAck disconnectAck = DisconnectAck
                    .builder()
                    .success(true)
                    .reasonCode(Mqtt5DisConnReasonCode.NORMAL)
                    .build();
            connection.getAckHandler()
                    .sendDisconnectAck(connection, disconnectAck);
        }
    }

    @Override
    public void processConnectionLost() {
        try {
            if (connection == null) {
                channel.close();
                return;
            }
            String clientId = connection.getClientId();
            if (log.isDebugEnabled()) {
                log.debug("[Connection Lost] [{}] ", clientId);
            }
            metricsCollector.removeClient(NettyUtils.getAddress(channel));
            WillMessage willMessage = connection.getWillMessage();
            if (willMessage != null) {
                willMessageHandler.fireWillMessage(clientId, willMessage);
            }
            connectionManager.removeConnection(connection);
            mqttSubscriptionManager.removeSubscription(clientId);
            connection.close();
        } finally {
            inactiveFuture.complete(null);
        }
    }

    @Override
    public void processPingReq() {
        channel.writeAndFlush(pingResp());
    }

    @Override
    public void processSubscribe(MqttAdapterMessage adapter) {
        MqttSubscribeMessage msg = (MqttSubscribeMessage) adapter.getMqttMessage();
        final String clientId = connection.getClientId();
        final String userRole = connection.getUserRole();
        final int packetId = msg.variableHeader().messageId();
        AckHandler ackHandler = connection.getAckHandler();
        if (log.isDebugEnabled()) {
            log.debug("[Subscribe] [{}] msg: {}", clientId, msg);
        }
        if (!configuration.isMqttAuthorizationEnabled()) {
            if (log.isDebugEnabled()) {
                log.debug("[Subscribe] authorization is disabled, allowing client. CId={}}", clientId);
            }
            doSubscribe(msg);
        } else {
            List<CompletableFuture<Void>> authorizationFutures = new ArrayList<>();
            AtomicBoolean authorizedFlag = new AtomicBoolean(true);
            for (MqttTopicSubscription topic: msg.payload().topicSubscriptions()) {
                authorizationFutures.add(this.authorizationService.canConsumeAsync(TopicName.get(topic.topicName()),
                        userRole, new AuthenticationDataCommand(userRole), userRole).thenAccept((authorized) -> {
                            if (!authorized) {
                                authorizedFlag.set(false);
                                log.warn("[Subscribe] no authorization to sub topic={}, userRole={}, CId= {}",
                                        topic.topicName(), userRole, clientId);
                            }
                        }));
            }
            FutureUtil.waitForAll(authorizationFutures).thenAccept(__ -> {
                if (!authorizedFlag.get()) {
                    SubscribeAck subscribeAck = SubscribeAck
                            .builder()
                            .success(false)
                            .packetId(packetId)
                            .errorReason(MqttSubAckMessageHelper.ErrorReason.AUTHORIZATION_FAIL)
                            .build();
                    ackHandler.sendSubscribeAck(connection, subscribeAck);
                } else {
                    doSubscribe(msg);
                }
            });
        }
    }

    private CompletableFuture<Void> doSubscribe(MqttSubscribeMessage msg) {
        final int messageID = msg.variableHeader().messageId();
        Map<String, String> userProperties = MqttPropertyUtils
                .getUserProperties(msg.idAndPropertiesVariableHeader().properties());
        CommandSubscribe.InitialPosition initPosition =
                Optional.ofNullable(userProperties.get(PulsarProperties.InitialPosition.name()))
                .map(v -> CommandSubscribe.InitialPosition.valueOf(Integer.parseInt(v)))
                .orElse(CommandSubscribe.InitialPosition.Latest);
        AckHandler ackHandler = connection.getAckHandler();
        final List<MqttTopicSubscription> subTopics = topicSubscriptions(msg);
        boolean duplicated = mqttSubscriptionManager.addSubscriptions(connection.getClientId(), subTopics);
        if (duplicated) {
            SubscribeAck subscribeAck = SubscribeAck
                    .builder()
                    .success(false)
                    .packetId(messageID)
                    .errorReason(MqttSubAckMessageHelper.ErrorReason.UNSPECIFIED_ERROR)
                    .reasonStr("Duplicated subscribe")
                    .build();
            ackHandler.sendSubscribeAck(connection, subscribeAck);
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futureList = new ArrayList<>(subTopics.size());
        Optional<String> retainedTopic = Optional.empty();
        for (MqttTopicSubscription subTopic : subTopics) {
            if (MqttUtils.isRegexFilter(subTopic.topicName())) {
                registerRegexTopicFilterListener(subTopic);
            }
            if (!retainedTopic.isPresent()) {
                retainedTopic = retainedMessageHandler.getRetainedTopic(subTopic.topicName());
            }
            metricsCollector.addSub(subTopic.topicName());
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    subTopic.topicName(), configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                    pulsarService, configuration.getDefaultTopicDomain());
            CompletableFuture<Void> completableFuture = topicListFuture.thenCompose(topics -> {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (String topic : topics) {
                    CompletableFuture<Subscription> subFuture = PulsarTopicUtils
                            .getOrCreateSubscription(pulsarService, topic, connection.getClientId(),
                                    configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                                    configuration.getDefaultTopicDomain(), initPosition);
                    CompletableFuture<Void> result = subFuture.thenCompose(sub ->
                            createAndSubConsumer(sub, subTopic, topic));
                    futures.add(result);
                }
                return FutureUtil.waitForAll(Collections.unmodifiableList(futures));
            });
            futureList.add(completableFuture);
        }
        final Optional<String> finalRetainedTopic = retainedTopic;
        return FutureUtil.waitForAll(futureList).thenAccept(v -> {
            SubscribeAck subscribeAck = SubscribeAck
                    .builder()
                    .success(true)
                    .packetId(messageID)
                    .grantedQoses(subTopics.stream()
                            .map(MqttTopicSubscription::qualityOfService)
                            .collect(Collectors.toList()))
                    .build();
            ackHandler.sendSubscribeAck(connection, subscribeAck).addListener(listener -> {
                finalRetainedTopic.map(topic ->
                    channel.writeAndFlush(MqttMessageUtils
                            .createRetainedMessage(retainedMessageHandler.getRetainedMessage(topic))));
            });
        }).exceptionally(ex -> {
            Throwable realCause = FutureUtil.unwrapCompletionException(ex);
            if (realCause instanceof BrokerServiceException.TopicNotFoundException) {
                log.warn("[Subscribe] Topic filter [{}] Not found, the configuration [isAllowAutoTopicCreation={}]",
                        subTopics.stream().map(MqttTopicSubscription::topicName)
                                .collect(Collectors.joining(",")),
                        pulsarService.getConfig().isAllowAutoTopicCreation());
                SubscribeAck subscribeAck = SubscribeAck
                        .builder()
                        .success(false)
                        .packetId(messageID)
                        .errorReason(MqttSubAckMessageHelper.ErrorReason.UNSPECIFIED_ERROR)
                        .reasonStr("Topic not found")
                        .build();
                ackHandler.sendSubscribeAck(connection, subscribeAck);
            } else {
                log.error("[Subscribe] [{}] Failed to process MQTT subscribe.", connection.getClientId(), ex);
                SubscribeAck subscribeAck = SubscribeAck
                        .builder()
                        .success(false)
                        .packetId(messageID)
                        .errorReason(MqttSubAckMessageHelper.ErrorReason.UNSPECIFIED_ERROR)
                        .reasonStr("[ MOP ERROR ]" + realCause.getMessage())
                        .build();
                ackHandler.sendSubscribeAck(connection, subscribeAck);
            }
            return null;
        });
    }

    private void registerRegexTopicFilterListener(MqttTopicSubscription subTopic) {
        connection.addTopicChangeListener(new PulsarTopicChangeListener() {
            @Override
            public void onTopicLoad(TopicName changedTopicName) {
                if (changedTopicName.isPartitioned()) {
                    // Current don't support partitioned topic
                    return;
                }
                String subTopicName = subTopic.topicName();
                String pulsarTopicNameStr = PulsarTopicUtils.getPulsarTopicName(subTopicName,
                        configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                        true, TopicDomain.getEnum(configuration.getDefaultTopicDomain()));
                TopicName pulsarTopicName = TopicName.get(pulsarTopicNameStr);
                // Support user use namespace level regex
                if (!Objects.equals(changedTopicName.getNamespace(), pulsarTopicName.getNamespace())) {
                    return;
                }
                TopicFilter topicFilter = PulsarTopicUtils.getTopicFilter(subTopic.topicName());
                if (!topicFilter.test(Codec.decode(changedTopicName.getLocalName()))) {
                    return;
                }
                PulsarTopicUtils.getOrCreateSubscription(pulsarService,
                        changedTopicName.toString(), connection.getClientId(), configuration.getDefaultTenant(),
                        configuration.getDefaultNamespace(), configuration.getDefaultTopicDomain(),
                                CommandSubscribe.InitialPosition.Earliest)
                        .thenCompose(sub -> {
                            Optional<String> existConsumer = sub.getConsumers().stream().map(Consumer::consumerName)
                                    .filter(name -> Objects.equals(name, connection.getClientId()))
                                    .findAny();
                            if (existConsumer.isPresent()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return createAndSubConsumer(sub, subTopic, changedTopicName.toString());
                        })
                        .thenRun(() -> log.info("[{}] Subscribe new topic [{}] success", connection.getClientId(),
                                Codec.decode(changedTopicName.toString())))
                        .exceptionally(ex -> {
                            log.error("[{}][Subscribe] Fail to subscribe new topic [{}] "
                                            + "for topic filter [{}]", connection.getClientId(), changedTopicName,
                                    subTopic.topicName());
                            return null;
                        });
            }

            @Override
            public void onTopicUnload(TopicName topicName) {
                // NO-OP
            }
        });
    }

    private CompletableFuture<Void> createAndSubConsumer(Subscription sub,
                                                         MqttTopicSubscription subTopic,
                                                         String changedTopicName) {
        MQTTConsumer consumer = new MQTTConsumer(sub, subTopic.topicName(), changedTopicName, connection, serverCnx,
                subTopic.qualityOfService(), packetIdGenerator, outstandingPacketContainer, metricsCollector);
        return sub.addConsumer(consumer).thenAccept(__ -> {
            consumer.addAllPermits();
            connection.getTopicSubscriptionManager().putIfAbsent(sub.getTopic(), sub, consumer);
        });
    }

    @Override
    public void processUnSubscribe(MqttAdapterMessage adapter) {
        final String clientId = connection.getClientId();
        if (log.isDebugEnabled()) {
            log.debug("[Unsubscribe] [{}] msg: {}", clientId, adapter);
        }
        final MqttUnsubscribeMessage msg = (MqttUnsubscribeMessage) adapter.getMqttMessage();
        final List<String> topicFilters = msg.payload().topics();
        final List<CompletableFuture<Void>> futureList = new ArrayList<>(topicFilters.size());
        for (String topicFilter : topicFilters) {
            metricsCollector.removeSub(topicFilter);
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    topicFilter, configuration.getDefaultTenant(), configuration.getDefaultNamespace(), pulsarService,
                    configuration.getDefaultTopicDomain());
            CompletableFuture<Void> future = topicListFuture.thenCompose(topics -> {
                List<CompletableFuture<Void>> futures = topics.stream()
                        .map(topic -> PulsarTopicUtils.getTopicReference(pulsarService,
                                topic, configuration.getDefaultTenant(), configuration.getDefaultNamespace(), false,
                                configuration.getDefaultTopicDomain(), false))
                        .map(topicFuture -> topicFuture.thenCompose(topicOp -> {
                            if (!topicOp.isPresent()) {
                                throw new MQTTTopicNotExistedException(
                                        String.format("Can not found topic when %s unSubscribe.", clientId));
                            }
                            return connection.getTopicSubscriptionManager()
                                    .unsubscribe(topicOp.get(), false);
                        }))
                        .collect(Collectors.toList());
                return FutureUtil.waitForAll(futures);
            });
            futureList.add(future);
        }
        int packetId = msg.variableHeader().messageId();
        AckHandler ackHandler = connection.getAckHandler();
        FutureUtil.waitForAll(futureList).thenAccept(__ -> {
            UnsubscribeAck unsubscribeAck = UnsubscribeAck
                    .builder()
                    .success(true)
                    .packetId(packetId)
                    .build();
            ackHandler.sendUnsubscribeAck(connection, unsubscribeAck);
        }).exceptionally(ex -> {
            log.error("[{}] Failed to process the UNSUB {}", clientId, msg);
            Throwable cause = ex.getCause();
            UnsubscribeAck unsubscribeAck;
            if (cause instanceof MQTTNoSubscriptionExistedException) {
                unsubscribeAck = UnsubscribeAck
                        .builder()
                        .success(true)
                        .packetId(packetId)
                        .reasonCode(Mqtt5UnsubReasonCode.NO_SUBSCRIPTION_EXISTED)
                        .build();
            } else {
                unsubscribeAck = UnsubscribeAck
                        .builder()
                        .success(false)
                        .packetId(packetId)
                        .reasonCode(Mqtt5UnsubReasonCode.UNSPECIFIED_ERROR)
                        .reasonString(cause.getMessage())
                        .build();
            }
            ackHandler.sendUnsubscribeAck(connection, unsubscribeAck);
            return null;
        });
    }
}
