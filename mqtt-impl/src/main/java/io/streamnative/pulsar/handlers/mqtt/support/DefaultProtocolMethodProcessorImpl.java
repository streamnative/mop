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
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.topicSubscriptions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
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
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.MQTTSubscriptionManager;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.MopExceptionHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttUnsubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
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

/**
 * Default implementation of protocol method processor.
 */
@Slf4j
public class DefaultProtocolMethodProcessorImpl implements ProtocolMethodProcessor {
    private final PulsarService pulsarService;
    private final QosPublishHandlers qosPublishHandlers;
    private final MQTTServerConfiguration configuration;
    private final MQTTServerCnx serverCnx;
    private final PacketIdGenerator packetIdGenerator;
    private final OutstandingPacketContainer outstandingPacketContainer;
    private final MQTTAuthenticationService authenticationService;
    private final AuthorizationService authorizationService;
    private final MQTTMetricsCollector metricsCollector;
    private final MQTTConnectionManager connectionManager;
    private final MQTTSubscriptionManager subscriptionManager;

    public DefaultProtocolMethodProcessorImpl (MQTTService mqttService, ChannelHandlerContext ctx) {
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
    }

    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        final int protocolVersion = msg.variableHeader().version();
        String clientId = payload.clientIdentifier();
        String username = payload.userName();
        if (log.isDebugEnabled()) {
            log.debug("process CONNECT message. CId={}, username={}", clientId, username);
        }

        // Check MQTT protocol version.
        if (!MqttUtils.isSupportedVersion(msg.variableHeader().version())) {
            MqttMessage badProto = MqttConnAckMessageHelper.
                    createMqtt(Mqtt5ConnReasonCode.UNSUPPORTED_PROTOCOL_VERSION);

            log.error("MQTT protocol version is not valid. CId={}", clientId);
            channel.writeAndFlush(badProto);
            channel.close();
            return;
        }

        // Client must specify the client ID except enable clean session on the connection.
        if (StringUtils.isEmpty(clientId)) {
            if (!msg.variableHeader().isCleanSession()) {
                MqttMessage badId = MqttUtils.isMqtt5(protocolVersion)
                        ? MqttConnAckMessageHelper.createMqtt(Mqtt5ConnReasonCode.CLIENT_IDENTIFIER_NOT_VALID) :
                        MqttConnAckMessageHelper.createMqtt(
                                Mqtt3ConnReasonCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
                channel.writeAndFlush(badId);
                channel.close();
                log.error("The MQTT client ID cannot be empty. Username={}", username);
                return;
            }

            clientId = MqttMessageUtils.createClientIdentifier(channel);
            if (log.isDebugEnabled()) {
                log.debug("Client has connected with generated identifier. CId={}", clientId);
            }
        }

        String userRole = null;
        // Authenticate the client
        if (!configuration.isMqttAuthenticationEnabled()) {
            log.info("Authentication is disabled, allowing client. CId={}, username={}", clientId, username);
        } else {
            MQTTAuthenticationService.AuthenticationResult authResult = authenticationService.authenticate(payload);
            if (authResult.isFailed()) {
                MqttMessage connectAuthenticationFailMessage = MqttUtils.isMqtt5(protocolVersion)
                        ? MqttConnAckMessageHelper.createMqtt(Mqtt5ConnReasonCode.BAD_USERNAME_OR_PASSWORD)
                        : MqttConnAckMessageHelper.createMqtt(
                                Mqtt3ConnReasonCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                channel.writeAndFlush(connectAuthenticationFailMessage);
                channel.close();
                log.error("Invalid or incorrect authentication. CId={}, username={}", clientId, username);
                return;
            }
            userRole = authResult.getUserRole();
        }

        NettyUtils.setClientId(channel, clientId);
        NettyUtils.setCleanSession(channel, msg.variableHeader().isCleanSession());
        NettyUtils.setUserRole(channel, userRole);
        NettyUtils.addIdleStateHandler(channel, MqttMessageUtils.getKeepAliveTime(msg));
        NettyUtils.setProtocolVersion(channel, protocolVersion);
        if (msg.variableHeader().isWillFlag()) {
            // Check willing message [ MQTT5 ]
            if (MqttUtils.isMqtt5(protocolVersion)) {
                int willQos = msg.variableHeader().willQos();
                MqttQoS mqttQoS = MqttQoS.valueOf(willQos);
                if (mqttQoS == MqttQoS.FAILURE || mqttQoS == MqttQoS.EXACTLY_ONCE) {
                    MqttMessage mqttConnAckMessage =
                            MqttConnAckMessageHelper.createMqtt5(
                                    Mqtt5ConnReasonCode.QOS_NOT_SUPPORTED,
                                    "The server do not support will message that qos is exactly once.");
                    channel.writeAndFlush(mqttConnAckMessage);
                    channel.close();
                }
            }
            NettyUtils.setWillMessage(channel, createWillMessage(msg));
        }
        metricsCollector.addClient(NettyUtils.getAndSetAddress(channel));
        MqttProperties properties = msg.variableHeader().properties();
        // Get receive maximum number.
        Integer receiveMaximum = MqttPropertyUtils.getReceiveMaximum(properties)
                .orElse(MqttUtils.isMqtt5(protocolVersion)
                        ? MqttPropertyUtils.MQTT5_DEFAULT_RECEIVE_MAXIMUM :
                        MqttPropertyUtils.BEFORE_DEFAULT_RECEIVE_MAXIMUM);
        Connection.ConnectionBuilder connectionBuilder = Connection.builder()
                .clientId(clientId)
                .protocolVersion(protocolVersion)
                .channel(channel)
                .manager(connectionManager)
                .clientReceiveMaximum(receiveMaximum)
                .serverReceivePubMaximum(configuration.getReceiveMaximum())
                .cleanSession(msg.variableHeader().isCleanSession());
        MqttPropertyUtils.getExpireInterval(properties).ifPresent(connectionBuilder::sessionExpireInterval);


        Connection connection = connectionBuilder.build();
        connectionManager.addConnection(connection);
        NettyUtils.setConnection(channel, connection);
        connection.sendConnAck();
    }

    @Override
    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
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
    public void processPublish(Channel channel, MqttPublishMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Publish] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }

        String clientID = NettyUtils.getClientId(channel);
        String userRole = NettyUtils.getUserRole(channel);
        final int protocolVersion = NettyUtils.getProtocolVersion(channel);
        final int packetId = msg.variableHeader().packetId();

        // Authorization the client
        if (!configuration.isMqttAuthorizationEnabled()) {
            log.info("[Publish] authorization is disabled, allowing client. CId={}, userRole={}", clientID, userRole);
            doPublish(channel, msg);
        } else {
            this.authorizationService.canProduceAsync(TopicName.get(msg.variableHeader().topicName()),
                    userRole, new AuthenticationDataCommand(userRole))
                    .thenAccept((authorized) -> {
                        if (!authorized) {
                            log.error("[Publish] no authorization to pub topic={}, userRole={}, CId= {}",
                                    msg.variableHeader().topicName(), userRole, clientID);
                            // Support Mqtt 5
                            if (MqttUtils.isMqtt5(protocolVersion)) {
                                MqttMessage mqttPubAckMessage =
                                    MqttPubAckMessageHelper.createMqtt5(packetId, Mqtt5PubReasonCode.NOT_AUTHORIZED,
                                        String.format("The client %s not authorized.", clientID));
                                channel.writeAndFlush(mqttPubAckMessage);
                            }
                            channel.close();
                        } else {
                            doPublish(channel, msg);
                        }
                    });
        }
    }

    private void doPublish(Channel channel, MqttPublishMessage msg) {
        boolean isMqtt5 = MqttUtils.isMqtt5(NettyUtils.getProtocolVersion(channel));
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        metricsCollector.addSend(msg.payload().readableBytes());
        switch (qos) {
            case AT_MOST_ONCE:
                this.qosPublishHandlers.qos0().publish(channel, msg);
                break;
            case AT_LEAST_ONCE:
                if (isMqtt5) {
                    checkServerReceivePubMessageAndIncrementCounterIfNeeded(channel, msg);
                }
                this.qosPublishHandlers.qos1().publish(channel, msg);
                break;
            case EXACTLY_ONCE:
                if (isMqtt5) {
                    checkServerReceivePubMessageAndIncrementCounterIfNeeded(channel, msg);
                }
                this.qosPublishHandlers.qos2().publish(channel, msg);
                break;
            default:
                log.error("Unknown QoS-Type:{}", qos);
                channel.close();
                break;
        }
    }

    private void checkServerReceivePubMessageAndIncrementCounterIfNeeded(Channel channel, MqttPublishMessage msg) {
        Connection connection = NettyUtils.getConnection(channel);
        if (connection.getServerReceivePubMessage() >= connection.getServerReceivePubMaximum()){
            log.warn("Client publish exceed server receive maximum , the receive maximum is {}",
                    connection.getServerReceivePubMaximum());
            int packetId = msg.variableHeader().packetId();
            MqttMessage quotaExceededPubAck =
                    MqttPubAckMessageHelper.createMqtt5(packetId, Mqtt5PubReasonCode.QUOTA_EXCEEDED);
            channel.writeAndFlush(quotaExceededPubAck);
            channel.close();
        } else {
            connection.incrementServerReceivePubMessage();
            log.info("increment pubmessage");
        }
    }

    @Override
    public void processPubRel(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRel] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }
    }

    @Override
    public void processPubRec(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }
    }

    @Override
    public void processPubComp(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}] msg: {}", NettyUtils.getClientId(channel), msg);
        }
    }

    @Override
    public void processDisconnect(Channel channel, MqttMessage msg) {
        final String clientId = NettyUtils.getClientId(channel);
        Connection connection = NettyUtils.getConnection(channel);
        // when reset expire interval present, we need to reset session expire interval.
        Object header = msg.variableHeader();
        if (header instanceof MqttReasonCodeAndPropertiesVariableHeader) {
            MqttProperties properties = ((MqttReasonCodeAndPropertiesVariableHeader) header).properties();
            if (!checkAndUpdateSessionExpireIntervalIfNeed(channel, clientId, connection, properties)){
                // If the session expire interval value is illegal.
                return;
            }
        }
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

    private boolean checkAndUpdateSessionExpireIntervalIfNeed(Channel channel, String clientId,
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
                channel.writeAndFlush(mqttPubAckMessage);
                // close the channel
                channel.close();
                return false;
            }
            connection.updateSessionExpireInterval(sessionExpireInterval);
        }
        return true;
    }

    @Override
    public void processConnectionLost(Channel channel) {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Connection Lost] [{}] ", clientId);
        }
        if (StringUtils.isNotEmpty(clientId)) {
            metricsCollector.removeClient(NettyUtils.getAddress(channel));
            Connection connection =  NettyUtils.getConnection(channel);
            if (connection != null) {
                connectionManager.removeConnection(connection);
                connection.removeSubscriptions();
            }
            subscriptionManager.removeSubscription(clientId);
            Optional<WillMessage> willMessage = NettyUtils.getWillMessage(channel);
            if (willMessage.isPresent()) {
                fireWillMessage(willMessage.get());
            }
        }
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
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientID = NettyUtils.getClientId(channel);
        String userRole = NettyUtils.getUserRole(channel);
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Subscribe] [{}] msg: {}", clientID, msg);
        }

        if (StringUtils.isEmpty(clientID)) {
            log.error("clientId is empty for sub [{}] close channel", msg);
            MqttMessage subAckMessage = MqttUtils.isMqtt5(protocolVersion)
                    ? MqttSubAckMessageHelper.createMqtt5(msg.variableHeader().messageId(),
                    Mqtt5SubReasonCode.UNSPECIFIED_ERROR, "The client id not found.") :
                    MqttSubAckMessageHelper.createMqtt(msg.variableHeader().messageId(), Mqtt3SubReasonCode.FAILURE);
            channel.writeAndFlush(subAckMessage);
            channel.close();
            return;
        }

        // Authorization the client
        if (!configuration.isMqttAuthorizationEnabled()) {
            log.info("[Subscribe] authorization is disabled, allowing client. CId={}, userRole={}", clientID, userRole);
            doSubscribe(channel, msg, clientID);
        } else {
            List<CompletableFuture<Void>> authorizationFutures = new ArrayList<>();
            AtomicBoolean authorizedFlag = new AtomicBoolean(true);
            for (MqttTopicSubscription topic: msg.payload().topicSubscriptions()) {
                authorizationFutures.add(this.authorizationService.canConsumeAsync(TopicName.get(topic.topicName()),
                        userRole, new AuthenticationDataCommand(userRole), userRole).thenAccept((authorized) -> {
                            if (!authorized) {
                                authorizedFlag.set(authorized);
                                log.warn("[Subscribe] no authorization to sub topic={}, userRole={}, CId= {}",
                                        topic.topicName(), userRole, clientID);
                            }
                        }));
            }
            FutureUtil.waitForAll(authorizationFutures).thenAccept(__ -> {
                if (!authorizedFlag.get()) {
                    int messageId = msg.variableHeader().messageId();
                    MqttMessage subscribeAckMessage = MqttUtils.isMqtt5(protocolVersion)
                            ? MqttSubAckMessageHelper.createMqtt5(messageId, Mqtt5SubReasonCode.NOT_AUTHORIZED,
                            String.format("The client %s not authorized.", clientID)) :
                            MqttSubAckMessageHelper.createMqtt(messageId, Mqtt3SubReasonCode.FAILURE);
                    channel.writeAndFlush(subscribeAckMessage);
                    channel.close();
                } else {
                    doSubscribe(channel, msg, clientID);
                }
            });
        }
    }

    private void doSubscribe(Channel channel, MqttSubscribeMessage msg, String clientID) {
        int messageID = msg.variableHeader().messageId();
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
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        FutureUtil.waitForAll(futureList).thenAccept(v -> {
            MqttMessage ackMessage =
                    // Support MQTT 5
                    MqttUtils.isMqtt5(protocolVersion) ? MqttSubAckMessageHelper.createMqtt5(messageID, subTopics) :
                            MqttSubAckMessageHelper.createMqtt(messageID, subTopics);
            if (log.isDebugEnabled()) {
                log.debug("Sending SUB-ACK message {} to {}", ackMessage, clientID);
            }
            channel.writeAndFlush(ackMessage);
            Map<Topic, Pair<Subscription, Consumer>> existedSubscriptions = NettyUtils.getTopicSubscriptions(channel);
            if (existedSubscriptions != null) {
                topicSubscriptions.putAll(existedSubscriptions);
            }
            NettyUtils.setTopicSubscriptions(channel, topicSubscriptions);
        }).exceptionally(e -> {
            log.error("[{}] Failed to process MQTT subscribe.", clientID, e);
            MopExceptionHelper.handle(MqttMessageType.SUBSCRIBE, messageID, channel, e);
            return null;
        });
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        String clientID = NettyUtils.getClientId(channel);
        Connection connection = NettyUtils.getConnection(channel);
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
                            throw new MQTTNoSubscriptionExistedException(
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
        int messageID = msg.variableHeader().messageId();
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        FutureUtil.waitForAll(futureList).thenAccept(__ -> {
            // ack the client
            MqttMessage ackMessage = MqttUtils.isMqtt5(protocolVersion) ?  // Support Mqtt version 5.0 reason code.
                    MqttUnsubAckMessageHelper.createMqtt5(messageID, Mqtt5UnsubReasonCode.SUCCESS) :
                    MqttUnsubAckMessageHelper.createMqtt(messageID);
            if (log.isDebugEnabled()) {
                log.debug("Sending UNSUBACK message {} to {}", ackMessage, clientID);
            }
            channel.writeAndFlush(ackMessage);
        }).exceptionally(ex -> {
            log.error("[{}] Failed to process the UNSUB {}", clientID, msg);
            MopExceptionHelper.handle(MqttMessageType.UNSUBSCRIBE, messageID, channel, ex);
            return null;
        });
    }
}
