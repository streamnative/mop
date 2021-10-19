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

import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.ESTABLISHED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.SENDACK;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createSubAckMessage;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.topicSubscriptions;
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
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor;
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptorStore;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;
import io.streamnative.pulsar.handlers.mqtt.utils.AuthUtils;
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
import javax.naming.AuthenticationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerServiceException;
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
    private final Map<String, AuthenticationProvider> authProviders;
    private final AuthorizationService authorizationService;
    private final MQTTMetricsCollector metricsCollector;

    public DefaultProtocolMethodProcessorImpl (PulsarService pulsarService, MQTTServerConfiguration configuration,
                                               Map<String, AuthenticationProvider> authProviders,
                                               AuthorizationService authorizationService,
                                               MQTTMetricsCollector metricsCollector, ChannelHandlerContext ctx) {

        this.pulsarService = pulsarService;
        this.configuration = configuration;
        this.qosPublishHandlers = new QosPublishHandlersImpl(pulsarService, configuration);
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.outstandingPacketContainer = new OutstandingPacketContainerImpl();
        this.authProviders = authProviders;
        this.authorizationService = authorizationService;
        this.metricsCollector = metricsCollector;
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
            boolean authenticated = false;
            for (Map.Entry<String, AuthenticationProvider> entry : this.authProviders.entrySet()) {
                String authMethod = entry.getKey();
                try {
                    userRole = entry.getValue().authenticate(AuthUtils.getAuthData(authMethod, payload));
                    authenticated = true;
                    if (log.isDebugEnabled()) {
                        log.debug("Authenticated with method: {}, role: {}. CId={}, username={}",
                                authMethod, userRole, clientId, username);
                    }
                    break;
                } catch (AuthenticationException e) {
                    log.info("Authentication failed with method: {}. CId={}, username={}",
                             authMethod, clientId, username
                    );
                }
            }
            if (!authenticated) {
                MqttConnAckMessage connAck = MqttMessageUtils.
                        connAck(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                channel.writeAndFlush(connAck);
                channel.close();
                log.error("Invalid or incorrect authentication. CId={}, username={}", clientId, username);
                return;
            }
        }

        ConnectionDescriptor descriptor = new ConnectionDescriptor(clientId, channel,
                msg.variableHeader().isCleanSession());
        ConnectionDescriptor existing = ConnectionDescriptorStore.getInstance().addConnection(descriptor);
        if (existing != null) {
            if (log.isDebugEnabled()) {
                log.debug("The client ID is being used in an existing connection. It will be closed. CId={}", clientId);
            }
            existing.abort();
            sendAck(descriptor, MqttConnectReturnCode.CONNECTION_ACCEPTED, clientId);
            return;
        }

        NettyUtils.attachClientID(channel, clientId);
        NettyUtils.attachUserRole(channel, userRole);
        NettyUtils.addIdleStateHandler(channel, MqttMessageUtils.getKeepAliveTime(msg));

        if (!sendAck(descriptor, MqttConnectReturnCode.CONNECTION_ACCEPTED, clientId)) {
            channel.close();
            return;
        }

        final boolean success = descriptor.assignState(SENDACK, ESTABLISHED);
        metricsCollector.addClient(NettyUtils.getAndAttachAddress(channel));

        if (log.isDebugEnabled()) {
            log.debug("The CONNECT message has been processed. CId={}, username={} success={}",
                    clientId, username, success);
        }
    }

    @Override
    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
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
            log.debug("[Publish] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
        }

        String clientID = NettyUtils.retrieveClientId(channel);
        String userRole = NettyUtils.retrieveUserRole(channel);
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
            log.debug("[PubRel] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
        }
    }

    @Override
    public void processPubRec(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
        }
    }

    @Override
    public void processPubComp(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
        }
    }

    @Override
    public void processDisconnect(Channel channel, MqttMessage msg) {
        final String clientID = NettyUtils.retrieveClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", clientID);
        }
        metricsCollector.removeClient(NettyUtils.retrieveAddress(channel));
        final ConnectionDescriptor existingDescriptor = ConnectionDescriptorStore.getInstance().getConnection(clientID);
        if (existingDescriptor == null) {
            // another client with same ID removed the descriptor, we must exit
            log.warn("connection descriptor is null. close CId={}", clientID);
            channel.close();
            return;
        }

        if (existingDescriptor.doesNotUseChannel(channel)) {
            // another client saved it's descriptor, exit
            log.warn("Another client is using the connection descriptor. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!existingDescriptor.close()) {
            log.warn("The connection has been closed. CId={}", clientID);
            return;
        }

        if (!removeSubscriptions(existingDescriptor, clientID)) {
            log.warn("Unable to remove subscriptions. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        boolean stillPresent = ConnectionDescriptorStore.getInstance().removeConnection(existingDescriptor);
        if (!stillPresent) {
            // another descriptor was inserted
            log.warn("Another descriptor has been inserted. CId={}", clientID);
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("The DISCONNECT message has been processed. CId={}", clientID);
        }
    }

    @Override
    public void processConnectionLost(Channel channel) {
        if (log.isDebugEnabled()) {
            log.debug("[Connection Lost] [{}] ", NettyUtils.retrieveClientId(channel));
        }
        String clientId = NettyUtils.retrieveClientId(channel);
        if (StringUtils.isNotEmpty(clientId)) {
            metricsCollector.removeClient(NettyUtils.retrieveAddress(channel));
            ConnectionDescriptor oldConnDescriptor = new ConnectionDescriptor(clientId, channel, true);
            ConnectionDescriptorStore.getInstance().removeConnection(oldConnDescriptor);
            removeSubscriptions(oldConnDescriptor, clientId);
        }
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Subscribe] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
        }
        String clientID = NettyUtils.retrieveClientId(channel);
        String userRole = NettyUtils.retrieveUserRole(channel);

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

        List<CompletableFuture<Void>> futureList = new ArrayList<>(subTopics.size());
        Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = new ConcurrentHashMap<>();
        for (MqttTopicSubscription subTopic : subTopics) {
            metricsCollector.addSub(subTopic.topicName());
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    subTopic.topicName(), configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                    pulsarService, configuration.getDefaultTopicDomain());
            CompletableFuture<Void> completableFuture = topicListFuture.thenCompose(topics -> {
                List<CompletableFuture<Subscription>> futures = new ArrayList<>();
                for (String topic : topics) {
                    CompletableFuture<Subscription> future = PulsarTopicUtils
                            .getOrCreateSubscription(pulsarService, topic, clientID,
                                    configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                                    configuration.getDefaultTopicDomain());
                    future.thenAccept(sub -> {
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
                    futures.add(future);
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
            NettyUtils.attachTopicSubscriptions(channel, topicSubscriptions);
        }).exceptionally(e -> {
            log.error("[{}] Failed to process MQTT subscribe.", clientID, e);
            channel.close();
            return null;
        });
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Unsubscribe] [{}] msg: {}", NettyUtils.retrieveClientId(channel), msg);
        }
        List<String> topicFilters = msg.payload().topics();
        String clientID = NettyUtils.retrieveClientId(channel);
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

    private boolean sendAck(ConnectionDescriptor descriptor, MqttConnectReturnCode returnCode, final String clientId) {
        if (log.isDebugEnabled()) {
            log.debug("Sending connect ACK. CId={}", clientId);
        }
        final boolean success = descriptor.assignState(DISCONNECTED, SENDACK);
        if (!success) {
            return false;
        }

        MqttConnAckMessage connAck = MqttMessageUtils.connAck(returnCode);

        descriptor.writeAndFlush(connAck);
        if (log.isDebugEnabled()) {
            log.debug("The connect ACK has been sent. CId={}", clientId);
        }
        return true;
    }

    private void removeConsumers(Channel channel) {
        Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = NettyUtils
                .retrieveTopicSubscriptions(channel);
        if (topicSubscriptions != null) {
            topicSubscriptions.forEach((k, v) -> {
                try {
                    v.getLeft().removeConsumer(v.getRight());
                } catch (BrokerServiceException ex) {
                    log.warn("subscription [{}] remove consumer {} error", v.getLeft(), v.getRight(), ex);
                }
            });
        }
    }

    private boolean removeSubscriptions(ConnectionDescriptor descriptor, String clientID) {
        if (descriptor != null) {
            removeConsumers(descriptor.getChannel());
        }
        return true;
    }
}
