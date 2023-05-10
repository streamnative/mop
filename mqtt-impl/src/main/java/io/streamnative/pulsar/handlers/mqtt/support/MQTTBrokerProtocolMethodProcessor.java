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
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttDisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttPubAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttSubAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.MqttUnsubAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.properties.PulsarProperties;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ServerRestrictions;
import io.streamnative.pulsar.handlers.mqtt.support.event.AutoSubscribeHandler;
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
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Default implementation of protocol method processor.
 */
@Slf4j
public class MQTTBrokerProtocolMethodProcessor extends AbstractCommonProtocolMethodProcessor {
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
    private final AutoSubscribeHandler autoSubscribeHandler;
    @Getter
    private final CompletableFuture<Void> inactiveFuture = new CompletableFuture<>();

    public MQTTBrokerProtocolMethodProcessor(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService.getAuthenticationService(),
                mqttService.getServerConfiguration().isMqttAuthenticationEnabled(), ctx);
        this.pulsarService = mqttService.getPulsarService();
        this.configuration = mqttService.getServerConfiguration();
        this.qosPublishHandlers = mqttService.getQosPublishHandlers();
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.outstandingPacketContainer = new OutstandingPacketContainerImpl();
        this.authorizationService = mqttService.getAuthorizationService();
        this.metricsCollector = mqttService.getMetricsCollector();
        this.connectionManager = mqttService.getConnectionManager();
        this.mqttSubscriptionManager = mqttService.getSubscriptionManager();
        this.willMessageHandler = mqttService.getWillMessageHandler();
        this.retainedMessageHandler = mqttService.getRetainedMessageHandler();
        this.serverCnx = new MQTTServerCnx(pulsarService, ctx);
        this.autoSubscribeHandler = new AutoSubscribeHandler(mqttService.getEventCenter());
    }

    @Override
    public void doProcessConnect(MqttAdapterMessage adapterMsg, String userRole,
                                 AuthenticationDataSource authData, ClientRestrictions clientRestrictions) {
        final MqttConnectMessage msg = (MqttConnectMessage) adapterMsg.getMqttMessage();
        ServerRestrictions serverRestrictions = ServerRestrictions.builder()
                .receiveMaximum(configuration.getReceiveMaximum())
                .maximumPacketSize(configuration.getMqttMessageMaxLength())
                .build();
        connection = Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(msg.payload().clientIdentifier())
                .userRole(userRole)
                .willMessage(createWillMessage(msg))
                .clientRestrictions(clientRestrictions)
                .serverRestrictions(serverRestrictions)
                .authData(authData)
                .channel(channel)
                .connectMessage(msg)
                .connectionManager(connectionManager)
                .fromProxy(adapterMsg.fromProxy())
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
            PositionImpl position;
            if (packet.isBatch()) {
                long[] ackSets = new long[packet.getBatchSize()];
                for (int i = 0; i < packet.getBatchSize(); i++) {
                    ackSets[i] = packet.getBatchIndex() == i ? 0 : 1;
                }
                position = PositionImpl.get(packet.getLedgerId(), packet.getEntryId(), ackSets);
            } else {
                position = PositionImpl.get(packet.getLedgerId(), packet.getEntryId());
            }
            packet.getConsumer().getSubscription().acknowledgeMessage(Collections.singletonList(position),
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
                            connection.getUserRole(), connection.getAuthData())
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
        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck
                .errorBuilder(connection.getProtocolVersion())
                .packetId(msg.variableHeader().packetId())
                .reasonCode(Mqtt5PubReasonCode.NOT_AUTHORIZED);
        if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
            pubAckBuilder.reasonString("Not Authorized!");
        }
        connection.sendAckThenClose(pubAckBuilder.build());
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> doPublish(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        metricsCollector.addSend(msg.payload().readableBytes());
        switch (qos) {
            case AT_MOST_ONCE:
                return this.qosPublishHandlers.qos0().publish(connection, adapter);
            case AT_LEAST_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(adapter);
                return this.qosPublishHandlers.qos1().publish(connection, adapter);
            case EXACTLY_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(adapter);
                return this.qosPublishHandlers.qos2().publish(connection, adapter);
            default:
                log.error("[Publish] Unknown QoS-Type:{}", qos);
                connection.getChannel().close();
                return FutureUtil.failedFuture(new IllegalArgumentException("Unknown Qos-Type: " + qos));
        }
    }

    private void checkServerReceivePubMessageAndIncrementCounterIfNeeded(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        // MQTT3 don't support this approach.
        if (MqttUtils.isMqtt3(connection.getProtocolVersion())) {
            return;
        }
        if (connection.getServerReceivePubMessage() >= connection.getClientRestrictions().getReceiveMaximum()) {
            log.warn("Client publish exceed server receive maximum , the receive maximum is {}",
                    connection.getServerRestrictions().getReceiveMaximum());
            MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck.errorBuilder(connection.getProtocolVersion())
                    .reasonCode(Mqtt5PubReasonCode.QUOTA_EXCEEDED)
                    .packetId(msg.variableHeader().packetId());
            if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                pubAckBuilder.reasonString(String.format("Publish exceed server receive maximum %s.",
                        connection.getServerRestrictions().getReceiveMaximum()));
            }
            connection.sendAckThenClose(pubAckBuilder.build());
        } else {
            connection.incrementServerReceivePubMessage();
        }
    }

    @Override
    public void processDisconnect(MqttAdapterMessage adapterMsg) {
        // When login, checkState(msg) failed, connection is null.
        final MqttMessage mqttMsg = adapterMsg.getMqttMessage();
        if (connection == null && !adapterMsg.fromProxy()) {
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
                .getUpdateSessionExpireIntervalIfExist(connection.getProtocolVersion(), mqttMsg)).isPresent()) {
            try {
                connection.updateSessionExpireInterval(newSessionExpireInterval.get());
            } catch (InvalidSessionExpireIntervalException e) {
                MqttAck disconnectAck = MqttDisconnectAck.errorBuilder(connection.getProtocolVersion())
                        .reasonCode(Mqtt5DisConnReasonCode.PROTOCOL_ERROR)
                        .reasonString(String.format("Disconnect with wrong session expire interval value."
                                + " the value is %s", newSessionExpireInterval))
                        .build();
                connection.sendAckThenClose(disconnectAck);
                return;
            }
        }
        MqttAck disconnectAck = MqttDisconnectAck.successBuilder(connection.getProtocolVersion())
                .build();
        connection.sendAckThenClose(disconnectAck);
    }

    @Override
    public void processConnectionLost() {
        try {
            autoSubscribeHandler.close();
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
            connection.cleanup();
        } finally {
            inactiveFuture.complete(null);
        }
    }

    @Override
    public void processPingReq(MqttAdapterMessage adapter) {
        connection.send(pingResp());
    }

    @Override
    public void processSubscribe(MqttAdapterMessage adapter) {
        MqttSubscribeMessage msg = (MqttSubscribeMessage) adapter.getMqttMessage();
        final String clientId = connection.getClientId();
        final String userRole = connection.getUserRole();
        final int packetId = msg.variableHeader().messageId();
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
                        userRole, connection.getAuthData(), userRole).thenAccept((authorized) -> {
                            if (!authorized) {
                                authorizedFlag.set(false);
                                log.warn("[Subscribe] no authorization to sub topic={}, userRole={}, CId= {}",
                                        topic.topicName(), userRole, clientId);
                            }
                        }));
            }
            FutureUtil.waitForAll(authorizationFutures).thenAccept(__ -> {
                if (!authorizedFlag.get()) {
                    MqttAck subAck = MqttSubAck.errorBuilder(connection.getProtocolVersion())
                            .packetId(packetId)
                            .errorReason(MqttSubAck.ErrorReason.AUTHORIZATION_FAIL)
                            .build();
                    connection.sendAckThenClose(subAck);
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
        final List<MqttTopicSubscription> subTopics = topicSubscriptions(msg);
        boolean duplicated = mqttSubscriptionManager.addSubscriptions(connection.getClientId(), subTopics);
        if (duplicated) {
            MqttSubAck.MqttSubErrorAckBuilder subAckBuilder = MqttSubAck.errorBuilder(connection.getProtocolVersion())
                    .packetId(messageID)
                    .errorReason(MqttSubAck.ErrorReason.UNSPECIFIED_ERROR);
            if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                subAckBuilder.reasonString("Duplicated subscribe");
            }
            connection.sendAckThenClose(subAckBuilder.build());
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
            MqttAck subAck = MqttSubAck.successBuilder(connection.getProtocolVersion())
                    .packetId(messageID)
                    .grantedQos(subTopics.stream()
                            .map(MqttTopicSubscription::qualityOfService)
                            .collect(Collectors.toList()))
                    .build();
            connection.sendAck(subAck)
                            .whenComplete((ignore1, ignore2)-> {
                                finalRetainedTopic.ifPresent(topic -> connection.send(MqttMessageUtils
                                        .createRetainedMessage(retainedMessageHandler.getRetainedMessage(topic))));
                            });
        }).exceptionally(ex -> {
            Throwable realCause = FutureUtil.unwrapCompletionException(ex);
            if (realCause instanceof BrokerServiceException.TopicNotFoundException) {
                log.warn("[Subscribe] Topic filter [{}] Not found, the configuration [isAllowAutoTopicCreation={}]",
                        subTopics.stream().map(MqttTopicSubscription::topicName)
                                .collect(Collectors.joining(",")),
                        pulsarService.getConfig().isAllowAutoTopicCreation());
                MqttSubAck.MqttSubErrorAckBuilder subAckBuilder =
                        MqttSubAck.errorBuilder(connection.getProtocolVersion())
                        .packetId(messageID)
                        .errorReason(MqttSubAck.ErrorReason.UNSPECIFIED_ERROR);
                if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                    subAckBuilder.reasonString("Topic not found");
                }
                connection.sendAckThenClose(subAckBuilder.build());
            } else {
                log.error("[Subscribe] [{}] Failed to process MQTT subscribe.", connection.getClientId(), ex);
                MqttSubAck.MqttSubErrorAckBuilder subAckBuilder =
                        MqttSubAck.errorBuilder(connection.getProtocolVersion())
                        .packetId(messageID)
                        .errorReason(MqttSubAck.ErrorReason.UNSPECIFIED_ERROR);
                if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                    subAckBuilder.reasonString("[ MOP ERROR ]" + realCause.getMessage());
                }
                connection.sendAckThenClose(subAckBuilder.build());
            }
            return null;
        });
    }

    private void registerRegexTopicFilterListener(MqttTopicSubscription subTopic) {
        autoSubscribeHandler.register(
                PulsarTopicUtils.getTopicFilter(subTopic.topicName()),
            (encodedChangedTopic) -> PulsarTopicUtils.getOrCreateSubscription(pulsarService,
                            encodedChangedTopic, connection.getClientId(), configuration.getDefaultTenant(),
                            configuration.getDefaultNamespace(), configuration.getDefaultTopicDomain(),
                            CommandSubscribe.InitialPosition.Earliest)
                    .thenCompose(sub -> {
                        Optional<String> existConsumer = sub.getConsumers().stream().map(Consumer::consumerName)
                                .filter(name -> Objects.equals(name, connection.getClientId()))
                                .findAny();
                        if (existConsumer.isPresent()) {
                            return CompletableFuture.completedFuture(null);
                        }
                        return createAndSubConsumer(sub, subTopic, encodedChangedTopic);
                    })
                    .thenRun(() -> log.info("[{}] Subscribe new topic [{}] success", connection.getClientId(),
                            Codec.decode(encodedChangedTopic)))
                    .exceptionally(ex -> {
                        log.error("[{}][Subscribe] Fail to subscribe new topic [{}] "
                                        + "for topic filter [{}]", connection.getClientId(), encodedChangedTopic,
                                subTopic.topicName());
                        return null;
                    }));
    }

    private CompletableFuture<Void> createAndSubConsumer(Subscription sub,
                                                         MqttTopicSubscription subTopic,
                                                         String pulsarTopicName) {
        MQTTConsumer consumer = new MQTTConsumer(sub, subTopic.topicName(), pulsarTopicName, connection, serverCnx,
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
            final boolean removed = mqttSubscriptionManager.removeSubscriptionForTopic(clientId, topicFilter);
            if (!removed) {
                throw new MQTTTopicNotExistedException(
                    String.format("Can not found topic when %s unsubscribe.", clientId));
            }
            metricsCollector.removeSub(topicFilter);
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    topicFilter, configuration.getDefaultTenant(), configuration.getDefaultNamespace(), pulsarService,
                    configuration.getDefaultTopicDomain());
            if (MqttUtils.isRegexFilter(topicFilter)) {
                TopicFilter filter = PulsarTopicUtils.getTopicFilter(topicFilter);
                autoSubscribeHandler.unregister(filter);
            }
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
        FutureUtil.waitForAll(futureList).thenAccept(__ -> {
            MqttAck unsubAck = MqttUnsubAck.successBuilder(connection.getProtocolVersion())
                    .packetId(packetId)
                    .build();
            connection.sendAck(unsubAck);
        }).exceptionally(ex -> {
            log.error("[{}] Failed to process the UNSUB {}", clientId, msg);
            Throwable cause = ex.getCause();
            if (cause instanceof MQTTNoSubscriptionExistedException) {
                MqttAck unsubAck = MqttUnsubAck.successBuilder(connection.getProtocolVersion())
                        .packetId(packetId)
                        .isNoSubscriptionExisted()
                        .build();
                connection.sendAck(unsubAck);
            } else {
                MqttUnsubAck.MqttUnsubErrorAckBuilder unsubAckBuilder =
                        MqttUnsubAck.errorBuilder(connection.getProtocolVersion())
                        .packetId(packetId)
                        .reasonCode(Mqtt5UnsubReasonCode.UNSPECIFIED_ERROR);
                if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                    unsubAckBuilder.reasonString(cause.getMessage());
                }
                connection.sendAckThenClose(unsubAckBuilder.build());
            }
            return null;
        });
    }
}
