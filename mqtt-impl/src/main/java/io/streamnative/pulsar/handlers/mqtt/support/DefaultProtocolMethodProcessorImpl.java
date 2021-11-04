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

import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createSubAckMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.topicSubscriptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        this.serverCnx = new MQTTServerCnx(pulsarService, ctx);
    }

    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        String username = payload.userName();
        if (log.isDebugEnabled()) {
            log.debug("process CONNECT message. CId={}, username={}", clientId, username);
        }

        // Check MQTT protocol version.
        if (msg.variableHeader().version() != MqttVersion.MQTT_3_1.protocolLevel()
                && msg.variableHeader().version() != MqttVersion.MQTT_3_1_1.protocolLevel()) {
            MqttConnAckMessage badProto = MqttMessageUtils.
                    connAck(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);

            log.error("MQTT protocol version is not valid. CId={}", clientId);
            channel.writeAndFlush(badProto);
            channel.close();
            return;
        }

        // Client must specify the client ID except enable clean session on the connection.
        if (StringUtils.isEmpty(clientId)) {
            if (!msg.variableHeader().isCleanSession()) {
                MqttConnAckMessage badId = MqttMessageUtils.
                        connAck(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);

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
                MqttConnAckMessage connAck = MqttMessageUtils.
                        connAck(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                channel.writeAndFlush(connAck);
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
        NettyUtils.setWillFlag(channel, false);
        NettyUtils.setWillTopic(channel, payload.willTopic());
        NettyUtils.setWillMessage(channel, payload.willMessageInBytes());
        metricsCollector.addClient(NettyUtils.getAndSetAddress(channel));

        Connection connection = new Connection(clientId, channel, msg.variableHeader().isCleanSession());
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
        NettyUtils.setWillFlag(channel, true);
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
                            MqttConnAckMessage connAck = MqttMessageUtils.
                                    connAck(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
                            channel.writeAndFlush(connAck);
                            channel.close();
                        } else {
                            doPublish(channel, msg);
                        }
                    });
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
                this.qosPublishHandlers.qos1().publish(channel, msg);
                break;
            case EXACTLY_ONCE:
                this.qosPublishHandlers.qos2().publish(channel, msg);
                break;
            default:
                log.error("Unknown QoS-Type:{}", qos);
                channel.close();
                break;
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
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", clientId);
        }
        metricsCollector.removeClient(NettyUtils.getAddress(channel));
        Connection connection =  NettyUtils.getConnection(channel);
        NettyUtils.setWillFlag(channel, false);
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
    public void processConnectionLost(Channel channel) {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Connection Lost] [{}] ", clientId);
        }
        if (StringUtils.isNotEmpty(clientId)) {
            metricsCollector.removeClient(NettyUtils.getAddress(channel));
            if (NettyUtils.getWillFlag(channel)) {
                publishWillMessage(channel);
            }
            Connection connection =  NettyUtils.getConnection(channel);
            if (connection != null) {
                connectionManager.removeConnection(connection);
                connection.removeSubscriptions();
            }
        }
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientID = NettyUtils.getClientId(channel);
        String userRole = NettyUtils.getUserRole(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Subscribe] [{}] msg: {}", clientID, msg);
        }

        if (StringUtils.isEmpty(clientID)) {
            log.error("clientId is empty for sub [{}] close channel", msg);
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
                    MqttConnAckMessage connAck = MqttMessageUtils.
                            connAck(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
                    channel.writeAndFlush(connAck);
                    channel.close();
                    return;
                } else {
                    doSubscribe(channel, msg, clientID);
                }
            });
        }
    }

    private void doSubscribe(Channel channel, MqttSubscribeMessage msg, String clientID) {
        int messageID = msg.variableHeader().messageId();
        List<MqttTopicSubscription> subTopics = topicSubscriptions(msg);
        if (StringUtils.isNotEmpty(NettyUtils.getWillTopic(channel))) {
            subTopics.add(new MqttTopicSubscription(NettyUtils.getWillTopic(channel), MqttQoS.AT_LEAST_ONCE));
        }

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
                                    outstandingPacketContainer, metricsCollector);
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
            MqttSubAckMessage ackMessage = createSubAckMessage(subTopics, messageID);
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
            channel.close();
            return null;
        });
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        String clientID = NettyUtils.getClientId(channel);
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
                            configuration.getDefaultTopicDomain()).thenAccept(topicOp -> {
                        if (topicOp.isPresent()) {
                            Subscription subscription = topicOp.get().getSubscription(clientID);
                            if (subscription != null) {
                                try {
                                    MQTTConsumer consumer = new MQTTConsumer(subscription, topicFilter,
                                        topic, clientID, serverCnx, qos, packetIdGenerator,
                                            outstandingPacketContainer, metricsCollector);
                                    topicOp.get().getSubscription(clientID).removeConsumer(consumer);
                                    futures.add(topicOp.get().unsubscribe(clientID));
                                } catch (Exception e) {
                                    throw new MQTTServerException(e);
                                }
                            }
                        }
                    });
                }
                return FutureUtil.waitForAll(futures);
            });
            futureList.add(future);
        }

        FutureUtil.waitForAll(futureList).thenAccept(__ -> {
            // ack the client
            int messageID = msg.variableHeader().messageId();
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE,
                    false, 0);
            MqttUnsubAckMessage ackMessage = new MqttUnsubAckMessage(fixedHeader,
                    MqttMessageIdVariableHeader.from(messageID));

            if (log.isDebugEnabled()) {
                log.debug("Sending UNSUBACK message {} to {}", ackMessage, clientID);
            }
            channel.writeAndFlush(ackMessage);
        }).exceptionally(ex -> {
            log.error("[{}] Failed to process the UNSUB {}", clientID, msg);
            channel.close();
            return null;
        });
    }

    private void publishWillMessage(Channel channel) {
        String willTopic = NettyUtils.getWillTopic(channel);
        if (StringUtils.isEmpty(willTopic)) {
            return;
        }
        byte[] willMessage = NettyUtils.getWillMessage(channel);
        ByteBuf message = PooledByteBufAllocator.DEFAULT.buffer(willMessage.length);
        message.writeBytes(willMessage);
        MqttPublishMessage willPublish = MessageBuilder.publish()
                .topicName(willTopic).payload(message)
                .qos(MqttQoS.AT_LEAST_ONCE).retained(true).build();
        doPublish(channel, willPublish);
        message.release();
    }
}
