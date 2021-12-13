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

import static io.netty.handler.codec.mqtt.MqttMessageType.CONNACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.PUBACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.SUBACK;
import static io.netty.handler.codec.mqtt.MqttMessageType.UNSUBACK;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createMqttWillMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createWillMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.pingResp;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.topicSubscriptions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.*;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTBadUserNameOrPasswordException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTClientIdentifierNotValidException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNotAuthorizedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.GlobalExceptionHandler;
import io.streamnative.pulsar.handlers.mqtt.messages.handler.ProtocolAckHandler;
import io.streamnative.pulsar.handlers.mqtt.messages.handler.ProtocolAckHandlerHelper;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTMetricsCollector;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTServerCnx;
import io.streamnative.pulsar.handlers.mqtt.support.OutstandingPacketContainerImpl;
import io.streamnative.pulsar.handlers.mqtt.support.QosPublishHandlersImpl;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.WillMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;


@Slf4j
public abstract class AbstractProtocolMethodProcessor implements ProtocolMethodProcessor {
    protected final PulsarService pulsarService;
    protected final QosPublishHandlers qosPublishHandlers;
    protected final MQTTServerConfiguration configuration;
    protected final MQTTServerCnx serverCnx;
    protected final PacketIdGenerator packetIdGenerator;
    protected final OutstandingPacketContainer outstandingPacketContainer;
    protected final MQTTAuthenticationService authenticationService;
    protected final AuthorizationService authorizationService;
    protected final MQTTMetricsCollector metricsCollector;
    protected final MQTTConnectionManager connectionManager;
    protected final MQTTSubscriptionManager subscriptionManager;
    protected final Channel channel;

    public AbstractProtocolMethodProcessor(MQTTService mqttService, ChannelHandlerContext ctx) {
        this.pulsarService = mqttService.getPulsarService();
        this.configuration = mqttService.getServerConfiguration();
        this.qosPublishHandlers = new QosPublishHandlersImpl(pulsarService, configuration);
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.outstandingPacketContainer = new OutstandingPacketContainerImpl();
        this.authenticationService = mqttService.getAuthenticationService();
        this.authorizationService = mqttService.getAuthorizationService();
        this.metricsCollector = mqttService.getMetricsCollector();
        this.connectionManager = mqttService.getConnectionManager();
        this.subscriptionManager = mqttService.getSubscriptionManager();
        this.serverCnx = new MQTTServerCnx(pulsarService, ctx);
        this.channel = ctx.channel();
    }

    abstract void checkWillingMessageIfNeeded(String clientId, int willQos);

    abstract Connection buildConnection(Connection.ConnectionBuilder connectionBuilder, MqttConnectMessage msg);

    abstract void checkServerReceivePubMessageAndIncrementCounterIfNeeded(Channel channel, MqttPublishMessage msg);

    @Override
    public void processConnect(MqttConnectMessage msg) {
        final int protocolVersion = msg.variableHeader().version();
        final MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        String username = payload.userName();
        // to prepare channel attribute
        NettyUtils.setProtocolVersion(channel, protocolVersion);
        NettyUtils.setCleanSession(channel, msg.variableHeader().isCleanSession());
        NettyUtils.addIdleStateHandler(channel, MqttMessageUtils.getKeepAliveTime(msg));
        ProtocolAckHandler ackHandler = ProtocolAckHandlerHelper.getAndCheckByProtocolVersion(channel);
        if (log.isDebugEnabled()) {
            log.debug("process CONNECT message. CId={}, username={}", clientId, username);
        }
        // start param check
        if (StringUtils.isEmpty(clientId)) {
            if (!msg.variableHeader().isCleanSession()) {
                throw new MQTTClientIdentifierNotValidException(username);
            }
            clientId = MqttMessageUtils.createClientIdentifier(channel);
            if (log.isDebugEnabled()) {
                log.debug("Client has connected with generated identifier. CId={}", clientId);
            }
        }
        String userRole = null;
        if (!configuration.isMqttAuthenticationEnabled()) {
            log.info("Authentication is disabled, allowing client. CId={}, username={}", clientId, username);
        } else {
            MQTTAuthenticationService.AuthenticationResult authResult = authenticationService.authenticate(payload);
            if (authResult.isFailed()) {
                throw new MQTTBadUserNameOrPasswordException(CONNACK, clientId, username);
            }
            userRole = authResult.getUserRole();
        }
        if (msg.variableHeader().isWillFlag()) {
            checkWillingMessageIfNeeded(clientId, msg.variableHeader().willQos());
            NettyUtils.setWillMessage(channel, createWillMessage(msg));
        }
        metricsCollector.addClient(NettyUtils.getAndSetAddress(channel));
        Connection.ConnectionBuilder connectionBuilder = Connection.builder()
                .clientId(clientId)
                .protocolVersion(protocolVersion)
                .channel(channel)
                .manager(connectionManager)
                .cleanSession(msg.variableHeader().isCleanSession());
        Connection connection = buildConnection(connectionBuilder, msg);
        connectionManager.addConnection(connection);
        // final to stuff data to channel
        NettyUtils.setClientId(channel, clientId);
        NettyUtils.setUserRole(channel, userRole);
        NettyUtils.setConnection(channel, connection);
        ackHandler.connOk(connection);
    }

    @Override
    public void processPubAck(MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
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
            log.debug("[Publish] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }

        String clientID = NettyUtils.getClientId(channel);
        String userRole = NettyUtils.getUserRole(channel);
        final int packetId = msg.variableHeader().packetId();
        String topicName = msg.variableHeader().topicName();
        if (log.isDebugEnabled()) {
            log.debug("[Publish] [{}] msg: {}", clientID, msg);
        }
        // Authorization the client
        if (!configuration.isMqttAuthorizationEnabled()) {
            log.info("[Publish] authorization is disabled, allowing client. CId={}, userRole={}", clientID, userRole);
            doPublish(channel, msg);
        } else {
            this.authorizationService.canProduceAsync(TopicName.get(topicName),
                            userRole, new AuthenticationDataCommand(userRole))
                    .thenAccept((authorized) -> {
                        if (!authorized) {
                            throw new MQTTNotAuthorizedException(PUBACK, packetId, topicName, userRole, clientID);
                        }
                        doPublish(channel, msg);
                    }).exceptionally(e -> {
                                GlobalExceptionHandler.handleServerException(channel, e.getCause());
                                return null;
                            }
                    );
        }
    }

    private void doPublish(Channel channel, MqttPublishMessage msg) {
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        metricsCollector.addSend(msg.payload().readableBytes());
        switch (qos) {
            case AT_MOST_ONCE:
                this.qosPublishHandlers.qos0().publish(channel, msg);
                break;
            case AT_LEAST_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(channel, msg);
                this.qosPublishHandlers.qos1().publish(channel, msg);
                break;
            case EXACTLY_ONCE:
                checkServerReceivePubMessageAndIncrementCounterIfNeeded(channel, msg);
                this.qosPublishHandlers.qos2().publish(channel, msg);
                break;
            default:
                log.error("Unknown QoS-Type:{}", qos);
                channel.close();
                break;
        }
    }


    @Override
    public void processPubRel(MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRel] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }
    }

    @Override
    public void processPubRec(MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }
    }

    @Override
    public void processPubComp(MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }
    }

    abstract void parseDisconnectPropertiesIfNeeded(MqttMessage msg);

    @Override
    public void processDisconnect(MqttMessage msg) {
        final String clientId = NettyUtils.getClientId(channel);
        Connection connection = NettyUtils.getConnection(channel);
        parseDisconnectPropertiesIfNeeded(msg);
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", clientId);
        }
        metricsCollector.removeClient(NettyUtils.getAddress(channel));
        // When login, checkState(msg) failed, connection is null.
        if (connection != null) {
            connectionManager.removeConnection(connection);
            connection.removeSubscriptions();
            connection.close();
        } else {
            log.warn("connection is null. close CId={}", clientId);
            channel.close();
        }
    }

    @Override
    public void processConnectionLost() {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Connection Lost] [{}] ", clientId);
        }
        if (StringUtils.isNotEmpty(clientId)) {
            metricsCollector.removeClient(NettyUtils.getAddress(channel));
            Connection connection = NettyUtils.getConnection(channel);
            if (connection != null) {
                connectionManager.removeConnection(connection);
                connection.removeSubscriptions();
            }
            subscriptionManager.removeSubscription(clientId);
            Optional<WillMessage> willMessage = NettyUtils.getWillMessage(channel);
            willMessage.ifPresent(this::fireWillMessage);
        }
    }

    @Override
    public void processPingReq() {
        channel.writeAndFlush(pingResp());
    }

    private void fireWillMessage(WillMessage willMessage) {
        List<Pair<String, String>> subscriptions = subscriptionManager.findMatchTopic(willMessage.getTopic());
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
        String clientID = NettyUtils.getClientId(channel);
        String userRole = NettyUtils.getUserRole(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Subscribe] [{}] msg: {}", clientID, msg);
        }
        // Authorization the client
        if (!configuration.isMqttAuthorizationEnabled()) {
            log.info("[Subscribe] authorization is disabled, allowing client. CId={}, userRole={}", clientID, userRole);
            doSubscribe(channel, msg, clientID);
        } else {
            List<CompletableFuture<Void>> authorizationFutures = new ArrayList<>();
            AtomicBoolean authorizedFlag = new AtomicBoolean(true);
            for (MqttTopicSubscription topic : msg.payload().topicSubscriptions()) {
                authorizationFutures.add(this.authorizationService.canConsumeAsync(TopicName.get(topic.topicName()),
                        userRole, new AuthenticationDataCommand(userRole), userRole).thenAccept((authorized) -> {
                    if (!authorized) {
                        authorizedFlag.set(false);
                        log.warn("[Subscribe] no authorization to sub topic={}, userRole={}, CId= {}",
                                topic.topicName(), userRole, clientID);
                    }
                }));
            }
            FutureUtil.waitForAll(authorizationFutures).thenAccept(__ -> {
                if (!authorizedFlag.get()) {
                    throw new MQTTNotAuthorizedException(SUBACK, msg.variableHeader().messageId(), userRole, clientID);
                }
                doSubscribe(channel, msg, clientID);
            }).exceptionally(e -> {
                GlobalExceptionHandler.handleServerException(channel, e.getCause());
                return null;
            });
        }
    }

    private void doSubscribe(Channel channel, MqttSubscribeMessage msg, String clientID) {
        ProtocolAckHandler ackHandler = ProtocolAckHandlerHelper.getAndCheckByProtocolVersion(channel);
        int packetId = msg.variableHeader().messageId();
        Connection connection = NettyUtils.getConnection(channel);
        List<MqttTopicSubscription> subTopics = topicSubscriptions(msg);
        subscriptionManager.addSubscriptions(NettyUtils.getClientId(channel), subTopics);
        List<CompletableFuture<Void>> futureList = new ArrayList<>(subTopics.size());
        Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = new ConcurrentHashMap<>();
        for (MqttTopicSubscription subTopic : subTopics) {
            metricsCollector.addSub(subTopic.topicName());
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    subTopic.topicName(), configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                    pulsarService, configuration.getDefaultTopicDomain());
            CompletableFuture<Void> completableFuture = topicListFuture.thenCompose(topics -> {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (String topic : topics) {
                    CompletableFuture<Subscription> subFuture = PulsarTopicUtils
                            .getOrCreateSubscription(pulsarService, topic, clientID,
                                    configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                                    configuration.getDefaultTopicDomain());
                    CompletableFuture<Void> result = subFuture.thenAccept(sub -> {
                        try {
                            MQTTConsumer consumer = new MQTTConsumer(sub, subTopic.topicName(), topic,
                                    clientID, serverCnx, subTopic.qualityOfService(), packetIdGenerator,
                                    outstandingPacketContainer, metricsCollector, connection.getClientReceiveMaximum());
                            sub.addConsumer(consumer);
                            consumer.addAllPermits();
                            topicSubscriptions.putIfAbsent(sub.getTopic(), Pair.of(sub, consumer));
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
        FutureUtil.waitForAll(futureList).thenAccept(v -> {
            List<MqttQoS> qosList = subTopics.stream().map(MqttTopicSubscription::qualityOfService)
                    .collect(Collectors.toList());
            ackHandler.subOk(connection, packetId, qosList);
            Map<Topic, Pair<Subscription, Consumer>> existedSubscriptions = NettyUtils.getTopicSubscriptions(channel);
            if (existedSubscriptions != null) {
                topicSubscriptions.putAll(existedSubscriptions);
            }
            NettyUtils.setTopicSubscriptions(channel, topicSubscriptions);
        }).exceptionally(ex -> {
            GlobalExceptionHandler.handleServerException(channel, ex.getCause())
                    .orElseHandleCommon(SUBACK, packetId);
            return null;
        });
    }

    @Override
    public void processUnSubscribe(MqttUnsubscribeMessage msg) {
        String clientID = NettyUtils.getClientId(channel);
        int packetId = msg.variableHeader().messageId();
        Connection connection = NettyUtils.getConnection(channel);
        ProtocolAckHandler ackHandler = ProtocolAckHandlerHelper.getAndCheckByProtocolVersion(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Unsubscribe] [{}] msg: {}", clientID, msg);
        }
        List<String> topicFilters = msg.payload().topics();
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        List<CompletableFuture<Void>> futureList = new ArrayList<>(topicFilters.size());
        for (String topicFilter : topicFilters) {
            metricsCollector.removeSub(topicFilter);
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    topicFilter, configuration.getDefaultTenant(), configuration.getDefaultNamespace(), pulsarService,
                    configuration.getDefaultTopicDomain());
            CompletableFuture<Void> future = topicListFuture.thenCompose(topics -> {
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (String topic : topics) {
                    PulsarTopicUtils.getTopicReference(pulsarService, topic, configuration.getDefaultTenant(),
                            configuration.getDefaultNamespace(), false,
                            configuration.getDefaultTopicDomain(), false).thenAccept(topicOp -> {
                        if (!topicOp.isPresent()) {
                            throw new MQTTTopicNotExistedException(
                                    String.format("Can not found topic %s when %s unSubscribe.", topic,
                                            clientID));
                        }
                        Subscription subscription = topicOp.get().getSubscription(clientID);
                        if (subscription == null) {
                            throw new MQTTNoSubscriptionExistedException(packetId,
                                    String.format(
                                            "Can not found subscription %s when %s unSubscribe. the "
                                                    + "topic is "
                                                    + "%s",
                                            clientID, clientID, topic));
                        }
                        try {
                            MQTTConsumer consumer = new MQTTConsumer(subscription, topicFilter,
                                    topic, clientID, serverCnx, qos, packetIdGenerator,
                                    outstandingPacketContainer, metricsCollector, connection.getClientReceiveMaximum());
                            topicOp.get().getSubscription(clientID).removeConsumer(consumer);
                            futures.add(topicOp.get().unsubscribe(clientID));
                        } catch (Exception e) {
                            throw new MQTTServerException(e);
                        }
                    }).exceptionally(ex -> {
                        futures.add(FutureUtil.failedFuture(ex));
                        return null;
                    });
                }
                return FutureUtil.waitForAll(futures);
            });
            futureList.add(future);
        }
        FutureUtil.waitForAll(futureList)
                .thenAccept(__ -> ackHandler.unSubOk(connection, packetId))
                .exceptionally(ex -> {
                    GlobalExceptionHandler.handleServerException(channel, ex.getCause())
                            .orElseHandleCommon(UNSUBACK, packetId);
                    return null;
                });
    }
}
