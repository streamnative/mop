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

import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createMqttWillMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createWillMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.pingResp;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.topicSubscriptions;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
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
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.MopExceptionHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.SessionExpireInterval;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttUnsubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.WillMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.naming.TopicName;
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
    private Connection connection;

    public DefaultProtocolMethodProcessorImpl(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService.getAuthenticationService(),
                mqttService.getServerConfiguration().isMqttAuthenticationEnabled(), ctx);
        this.pulsarService = mqttService.getPulsarService();
        this.configuration = mqttService.getServerConfiguration();
        this.qosPublishHandlers = new QosPublishHandlersImpl(pulsarService, configuration, ctx.channel());
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.outstandingPacketContainer = new OutstandingPacketContainerImpl();
        this.authorizationService = mqttService.getAuthorizationService();
        this.metricsCollector = mqttService.getMetricsCollector();
        this.connectionManager = mqttService.getConnectionManager();
        this.mqttSubscriptionManager = mqttService.getSubscriptionManager();
        this.serverCnx = new MQTTServerCnx(pulsarService, ctx);
    }

    @Override
    public void doProcessConnect(MqttConnectMessage msg, String userRole) {
        connection = Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(msg.payload().clientIdentifier())
                .userRole(userRole)
                .willMessage(createWillMessage(msg))
                .cleanSession(msg.variableHeader().isCleanSession())
                .sessionExpireInterval(MqttPropertyUtils.getExpireInterval(msg.variableHeader().properties())
                        .orElse(SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime()))
                .clientReceiveMaximum(MqttPropertyUtils.getReceiveMaximum(msg.variableHeader().version(),
                        msg.variableHeader().properties()))
                .serverReceivePubMaximum(configuration.getReceiveMaximum())
                .keepAliveTime(msg.variableHeader().keepAliveTimeSeconds())
                .channel(channel)
                .connectionManager(connectionManager)
                .build();
        metricsCollector.addClient(NettyUtils.getAndSetAddress(channel));
        connection.sendConnAck();
    }

    @Override
    public void processPubAck(MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}] msg: {}", connection.getClientId(), msg);
        }
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
    public void processPublish(MqttPublishMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Publish] [{}] msg: {}", connection.getClientId(), msg);
        }
        CompletableFuture<Void> result;
        if (!configuration.isMqttAuthorizationEnabled()) {
            if (log.isDebugEnabled()) {
                log.debug("[Publish] authorization is disabled, allowing client. CId={}, userRole={}",
                        connection.getClientId(), connection.getUserRole());
            }
            result = doPublish(msg);
        } else {
            result = this.authorizationService.canProduceAsync(TopicName.get(msg.variableHeader().topicName()),
                            connection.getUserRole(), new AuthenticationDataCommand(connection.getUserRole()))
                    .thenCompose(authorized -> authorized ? doPublish(msg) : doUnauthorized(msg));
        }
        result.thenAccept(__ -> msg.release())
              .exceptionally(ex -> {
                    log.error("[{}] Write {} to Pulsar topic failed.", msg.variableHeader().topicName(), msg, ex);
                    msg.release();
                    return null;
                });
    }

    private CompletableFuture<Void> doUnauthorized(MqttPublishMessage msg) {
        log.error("[Publish] not authorized to topic={}, userRole={}, CId= {}",
                msg.variableHeader().topicName(), connection.getUserRole(),
                connection.getClientId());
        // Support Mqtt 5
        if (MqttUtils.isMqtt5(connection.getProtocolVersion())) {
            MqttMessage mqttPubAckMessage =
                    MqttPubAckMessageHelper.createMqtt5(msg.variableHeader().packetId(),
                            Mqtt5PubReasonCode.NOT_AUTHORIZED,
                            String.format("The client %s not authorized.", connection.getClientId()));
            channel.writeAndFlush(mqttPubAckMessage);
        }
        channel.close();
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> doPublish(MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        metricsCollector.addSend(msg.payload().readableBytes());
        CompletableFuture<Void> result;
        switch (qos) {
            case AT_MOST_ONCE:
                result = this.qosPublishHandlers.qos0().publish(msg);
                break;
            case AT_LEAST_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(msg);
                result = this.qosPublishHandlers.qos1().publish(msg);
                break;
            case EXACTLY_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(msg);
                result = this.qosPublishHandlers.qos2().publish(msg);
                break;
            default:
                log.error("Unknown QoS-Type:{}", qos);
                channel.close();
                result = CompletableFuture.completedFuture(null);
                break;
        }
        return result;
    }

    private void checkServerReceivePubMessageAndIncrementCounterIfNeeded(MqttPublishMessage msg) {
        // check mqtt 5.0
        if (!MqttUtils.isMqtt5(connection.getProtocolVersion())) {
            return;
        }
        if (connection.getServerReceivePubMessage() >= connection.getServerReceivePubMaximum()) {
            log.warn("Client publish exceed server receive maximum , the receive maximum is {}",
                    connection.getServerReceivePubMaximum());
            MqttMessage quotaExceededPubAck =
                    MqttPubAckMessageHelper.createMqtt5(msg.variableHeader().packetId(),
                            Mqtt5PubReasonCode.QUOTA_EXCEEDED);
            connection.sendThenClose(quotaExceededPubAck);
        } else {
            connection.incrementServerReceivePubMessage();
        }
    }

    @Override
    public void processDisconnect(MqttMessage msg) {
        // When login, checkState(msg) failed, connection is null.
        if (connection == null) {
            channel.close();
            return;
        }
        final String clientId = connection.getClientId();
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", clientId);
        }
        // when reset expire interval present, we need to reset session expire interval.
        Object header = msg.variableHeader();
        if (header instanceof MqttReasonCodeAndPropertiesVariableHeader) {
            MqttProperties properties = ((MqttReasonCodeAndPropertiesVariableHeader) header).properties();
            if (!checkAndUpdateSessionExpireIntervalIfNeed(clientId, connection, properties)){
                // If the session expire interval value is illegal.
                return;
            }
        }
        metricsCollector.removeClient(NettyUtils.getAddress(channel));
        connection.close()
                .thenAccept(__ -> connectionManager.removeConnection(connection));
    }

    private boolean checkAndUpdateSessionExpireIntervalIfNeed(String clientId,
                                                              Connection connection, MqttProperties properties) {
        Optional<Integer> expireInterval = MqttPropertyUtils.getExpireInterval(properties);
        if (expireInterval.isPresent()) {
            Integer sessionExpireInterval = expireInterval.get();
            boolean checkResult = connection.checkIsLegalExpireInterval(sessionExpireInterval);
            if (!checkResult) {
                // the detail in mqtt 5 3.2.2.1.1
                MqttMessage mqttPubAckMessage =
                        MqttDisConnAckMessageHelper.createMqtt5(Mqtt5DisConnReasonCode.PROTOCOL_ERROR,
                                String.format("The client %s disconnect with wrong "
                                                + "session expire interval value. the value is %s",
                                        clientId, sessionExpireInterval));
                connection.sendThenClose(mqttPubAckMessage);
                return false;
            }
            connection.updateSessionExpireInterval(sessionExpireInterval);
        }
        return true;
    }

    @Override
    public void processConnectionLost() {
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
            fireWillMessage(willMessage);
        }
        connection.close()
                .thenAccept(__ -> {
                    connectionManager.removeConnection(connection);
                    mqttSubscriptionManager.removeSubscription(clientId);
                });
    }

    @Override
    public void processPingReq() {
        channel.writeAndFlush(pingResp());
    }

    private void fireWillMessage(WillMessage willMessage) {
        List<Pair<String, String>> subscriptions = mqttSubscriptionManager.findMatchTopic(willMessage.getTopic());
        MqttPublishMessage msg = createMqttWillMessage(willMessage);
        for (Pair<String, String> entry : subscriptions) {
            Connection connection = connectionManager.getConnection(entry.getLeft());
            if (connection != null) {
                connection.send(msg);
            } else {
                log.warn("Not find connection for empty : {}", entry.getLeft());
            }
        }
    }

    @Override
    public void processSubscribe(MqttSubscribeMessage msg) {
        final String clientId = connection.getClientId();
        final String userRole = connection.getUserRole();
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
                                authorizedFlag.set(authorized);
                                log.warn("[Subscribe] no authorization to sub topic={}, userRole={}, CId= {}",
                                        topic.topicName(), userRole, clientId);
                            }
                        }));
            }
            FutureUtil.waitForAll(authorizationFutures).thenAccept(__ -> {
                if (!authorizedFlag.get()) {
                    doUnauthorized(msg);
                } else {
                    doSubscribe(msg);
                }
            });
        }
    }

    private CompletableFuture<Void> doUnauthorized(MqttSubscribeMessage msg) {
        final String clientId = connection.getClientId();
        final int protocolVersion = connection.getProtocolVersion();
        final int messageId = msg.variableHeader().messageId();
        MqttMessage subscribeAckMessage = MqttUtils.isMqtt5(protocolVersion)
                ? MqttSubAckMessageHelper.createMqtt5(messageId, Mqtt5SubReasonCode.NOT_AUTHORIZED,
                String.format("The client %s not authorized.", clientId)) :
                MqttSubAckMessageHelper.createMqtt(messageId, Mqtt3SubReasonCode.FAILURE);
        connection.sendThenClose(subscribeAckMessage);
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> doSubscribe(MqttSubscribeMessage msg) {
        final int messageID = msg.variableHeader().messageId();
        final List<MqttTopicSubscription> subTopics = topicSubscriptions(msg);
        mqttSubscriptionManager.addSubscriptions(connection.getClientId(), subTopics);
        List<CompletableFuture<Void>> futureList = new ArrayList<>(subTopics.size());
        for (MqttTopicSubscription subTopic : subTopics) {
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
                                    configuration.getDefaultTopicDomain());
                    CompletableFuture<Void> result = subFuture.thenAccept(sub -> {
                        try {
                            MQTTConsumer consumer = new MQTTConsumer(sub, subTopic.topicName(), topic,
                                    connection.getClientId(), serverCnx, subTopic.qualityOfService(), packetIdGenerator,
                                    outstandingPacketContainer, metricsCollector, connection.getClientReceiveMaximum());
                            sub.addConsumer(consumer);
                            consumer.addAllPermits();
                            connection.getTopicSubscriptionManager().putIfAbsent(sub.getTopic(), sub, consumer);
                        } catch (Exception e) {
                            throw new MQTTServerException(e);
                        }
                    });
                    futures.add(result);
                }
                return FutureUtil.waitForAll(futures);
            });
            futureList.add(completableFuture);
        }
        return FutureUtil.waitForAll(futureList).thenAccept(v -> {
            MqttMessage ackMessage =
                    // Support MQTT 5
                    MqttUtils.isMqtt5(connection.getProtocolVersion())
                            ? MqttSubAckMessageHelper.createMqtt5(messageID, subTopics)
                            : MqttSubAckMessageHelper.createMqtt(messageID, subTopics);
            if (log.isDebugEnabled()) {
                log.debug("Sending SUB-ACK message {} to {}", ackMessage, connection.getClientId());
            }
            connection.send(ackMessage);
        }).exceptionally(e -> {
            log.error("[{}] Failed to process MQTT subscribe.", connection.getClientId(), e);
            MopExceptionHelper.handle(MqttMessageType.SUBSCRIBE, messageID, channel, e);
            return null;
        });
    }

    @Override
    public void processUnSubscribe(MqttUnsubscribeMessage msg) {
        final String clientId = connection.getClientId();
        if (log.isDebugEnabled()) {
            log.debug("[Unsubscribe] [{}] msg: {}", clientId, msg);
        }
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
        int messageId = msg.variableHeader().messageId();
        int protocolVersion = connection.getProtocolVersion();
        FutureUtil.waitForAll(futureList).thenAccept(__ -> {
            // ack the client
            MqttMessage ackMessage = MqttUtils.isMqtt5(protocolVersion) ?  // Support Mqtt version 5.0 reason code.
                    MqttUnsubAckMessageHelper.createMqtt5(messageId, Mqtt5UnsubReasonCode.SUCCESS) :
                    MqttUnsubAckMessageHelper.createMqtt(messageId);
            if (log.isDebugEnabled()) {
                log.debug("Sending UNSUBACK message {} to {}", ackMessage, clientId);
            }
            connection.send(ackMessage);
        }).exceptionally(ex -> {
            log.error("[{}] Failed to process the UNSUB {}", clientId, msg);
            MopExceptionHelper.handle(MqttMessageType.UNSUBSCRIBE, messageId, channel, ex);
            return null;
        });
    }
}
