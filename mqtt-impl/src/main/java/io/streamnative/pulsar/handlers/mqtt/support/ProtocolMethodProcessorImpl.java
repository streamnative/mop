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
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.SUBSCRIPTIONS_REMOVED;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
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
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor;
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptorStore;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;
import io.streamnative.pulsar.handlers.mqtt.utils.AuthUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.util.FutureUtil;


/**
 * Default implementation of protocol method processor.
 */
@Slf4j
public class ProtocolMethodProcessorImpl implements ProtocolMethodProcessor {
    private final PulsarService pulsarService;
    private final QosPublishHandlers qosPublishHandlers;
    private final MQTTServerConfiguration configuration;
    private MQTTServerCnx serverCnx;
    private final PacketIdGenerator packetIdGenerator;
    private final OutstandingPacketContainer outstandingPacketContainer;
    private final Map<String, AuthenticationProvider> authProviders;


    public ProtocolMethodProcessorImpl(
        PulsarService pulsarService,
        MQTTServerConfiguration configuration,
        Map<String, AuthenticationProvider> authProviders
    ) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
        this.qosPublishHandlers = new QosPublishHandlersImpl(pulsarService, configuration);
        this.packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.outstandingPacketContainer = new OutstandingPacketContainerImpl();
        this.authProviders = authProviders;
    }

    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        String username = payload.userName();
        log.info("process CONNECT message. CId={}, username={}", clientId, username);

        // Check MQTT protocol version.
        if (msg.variableHeader().version() != MqttVersion.MQTT_3_1.protocolLevel()
                && msg.variableHeader().version() != MqttVersion.MQTT_3_1_1.protocolLevel()) {
            MqttConnAckMessage badProto =
                    connAck(MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);

            log.error("MQTT protocol version is not valid. CId={}", clientId);
            channel.writeAndFlush(badProto);
            channel.close();
            return;
        }

        // Client must specify the client ID except enable clean session on the connection.
        if (clientId == null || clientId.length() == 0) {
            if (!msg.variableHeader().isCleanSession()) {
                MqttConnAckMessage badId = connAck(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);

                channel.writeAndFlush(badId);
                channel.close();
                log.error("The MQTT client ID cannot be empty. Username={}", username);
                return;
            }

            // Generating client id.
            clientId = UUID.randomUUID().toString().replace("-", "");
            log.info("Client has connected with a server generated identifier. CId={}, username={}", clientId,
                     username
            );
        }

        // Authenticate the client
        if (!configuration.isMqttAuthenticationEnabled()) {
            log.info("Authentication is disabled, allowing client. CId={}, username={}", clientId, username);
        } else {
            boolean authenticated = false;
            for (Map.Entry<String, AuthenticationProvider> entry : this.authProviders.entrySet()) {
                String authMethod = entry.getKey();
                try {
                    String authRole = entry.getValue().authenticate(AuthUtils.getAuthData(authMethod, payload));
                    authenticated = true;
                    log.info("Authenticated with method: {}, role: {}. CId={}, username={}",
                             authMethod, authRole, clientId, username);
                    break;
                } catch (AuthenticationException e) {
                    log.info("Authentication failed with method: {}. CId={}, username={}",
                             authMethod, clientId, username
                    );
                }
            }
            if (!authenticated) {
                channel.writeAndFlush(connAck(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD));
                channel.close();
                log.error("Invalid or incorrect authentication. CId={}, username={}", clientId, username);
                return;
            }
        }

        ConnectionDescriptor descriptor = new ConnectionDescriptor(clientId, channel,
                msg.variableHeader().isCleanSession());
        ConnectionDescriptor existing = ConnectionDescriptorStore.getInstance().addConnection(descriptor);
        if (existing != null) {
            log.info("The client ID is being used in an existing connection. It will be closed. CId={}", clientId);
            existing.abort();
            sendAck(descriptor, MqttConnectReturnCode.CONNECTION_ACCEPTED, clientId);
            return;
        }

        initializeKeepAliveTimeout(channel, msg, clientId);

        if (!sendAck(descriptor, MqttConnectReturnCode.CONNECTION_ACCEPTED, clientId)) {
            channel.close();
            return;
        }

        final boolean success = descriptor.assignState(SENDACK, ESTABLISHED);

        log.info("The CONNECT message has been processed. CId={}, username={} success={}",
                 clientId, username, success);
    }

    @Override
    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubAck] [{}] msg: {}", channel, msg);
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
            log.debug("[Publish] [{}] msg: {}", channel, msg);
        }
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        switch (qos) {
            case AT_MOST_ONCE:
                this.qosPublishHandlers.qos0().receivePublish(channel, msg);
                break;
            case AT_LEAST_ONCE:
                this.qosPublishHandlers.qos1().receivePublish(channel, msg);
                break;
            case EXACTLY_ONCE:
                this.qosPublishHandlers.qos2().receivePublish(channel, msg);
                break;
            default:
                log.error("Unknown QoS-Type:{}", qos);
                break;
        }
    }

    @Override
    public void processPubRel(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRel] [{}] msg: {}", channel, msg);
        }
    }

    @Override
    public void processPubRec(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubRec] [{}] msg: {}", channel, msg);
        }
    }

    @Override
    public void processPubComp(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[PubComp] [{}] msg: {}", channel, msg);
        }
    }

    @Override
    public void processDisconnect(Channel channel) throws InterruptedException {
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", channel);
        }
        final String clientID = NettyUtils.clientID(channel);
        log.info("Processing DISCONNECT message. CId={}", clientID);
        channel.flush();
        final ConnectionDescriptor existingDescriptor = ConnectionDescriptorStore.getInstance().getConnection(clientID);
        if (existingDescriptor == null) {
            // another client with same ID removed the descriptor, we must exit
            channel.close();
            return;
        }

        if (existingDescriptor.doesNotUseChannel(channel)) {
            // another client saved it's descriptor, exit
            log.warn("Another client is using the connection descriptor. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!removeSubscriptions(existingDescriptor, clientID)) {
            log.warn("Unable to remove subscriptions. Closing connection. CId={}", clientID);
            existingDescriptor.abort();
            return;
        }

        if (!existingDescriptor.close()) {
            log.info("The connection has been closed. CId={}", clientID);
            return;
        }

        boolean stillPresent = ConnectionDescriptorStore.getInstance().removeConnection(existingDescriptor);
        if (!stillPresent) {
            // another descriptor was inserted
            log.warn("Another descriptor has been inserted. CId={}", clientID);
            return;
        }

        log.info("The DISCONNECT message has been processed. CId={}", clientID);
    }

    @Override
    public void processConnectionLost(String clientID, Channel channel) {
        log.info("[Connection Lost] [{}] clientId: {}", channel, clientID);
        ConnectionDescriptor oldConnDescr = new ConnectionDescriptor(clientID, channel, true);
        ConnectionDescriptorStore.getInstance().removeConnection(oldConnDescr);
        removeSubscriptions(null, clientID);
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        log.info("[Subscribe] [{}] msg: {}", channel, msg);
        String clientID = NettyUtils.clientID(channel);
        int messageID = msg.variableHeader().messageId();
        String username = NettyUtils.userName(channel);

        List<MqttTopicSubscription> ackTopics = doVerify(clientID, username, msg);
        List<CompletableFuture<Subscription>> futures = new ArrayList<>();
        for (MqttTopicSubscription ackTopic : ackTopics) {
            CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                    ackTopic, configuration.getDefaultTenant(), configuration.getDefaultNamespace(), pulsarService);
            topicListFuture.thenAccept(topics -> {
                for (String topic : topics) {
                    CompletableFuture<Subscription> future = PulsarTopicUtils
                            .getOrCreateSubscription(pulsarService, topic, clientID,
                                    configuration.getDefaultTenant(), configuration.getDefaultNamespace());
                    future.thenAccept(sub -> {
                        try {
                            MQTTConsumer consumer = new MQTTConsumer(sub, topic,
                                    PulsarTopicUtils.getEncodedPulsarTopicName(topic,
                                            configuration.getDefaultTenant(), configuration.getDefaultNamespace()),
                                    clientID, serverCnx,
                                    ackTopic.qualityOfService(), packetIdGenerator, outstandingPacketContainer);
                            sub.addConsumer(consumer);
                            consumer.addAllPermits();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    futures.add(future);
                }

            });

        }
        FutureUtil.waitForAll(futures).thenAccept(v -> {
            MqttSubAckMessage ackMessage = doAckMessageFromValidateFilters(ackTopics, messageID);
            log.info("Sending SUBACK message {} to {}", ackMessage, clientID);
            channel.writeAndFlush(ackMessage);
        }).exceptionally(e -> {
            log.error("[{}] Failed to process MQTT subscribe.", clientID, e);
            channel.close();
            return null;
        });
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        log.info("[Unsubscribe] [{}] msg: {}", channel, msg);
        List<String> topics = msg.payload().topics();
        String clientID = NettyUtils.clientID(channel);
        final MqttQoS qos = msg.fixedHeader().qosLevel();

        for (String topic : topics) {
            PulsarTopicUtils.getTopicReference(pulsarService, topic, configuration.getDefaultTenant(),
                    configuration.getDefaultNamespace(), true).thenAccept(topicOp -> {
                if (topicOp.isPresent()) {
                    Subscription subscription = topicOp.get().getSubscription(clientID);
                    if (subscription != null) {
                        try {
                            MQTTConsumer consumer = new MQTTConsumer(subscription, topic,
                                    PulsarTopicUtils.getEncodedPulsarTopicName(topic, configuration.getDefaultTenant(),
                                            configuration.getDefaultNamespace()), clientID, serverCnx, qos,
                                    packetIdGenerator, outstandingPacketContainer);
                            topicOp.get().getSubscription(clientID).removeConsumer(consumer);
                            topicOp.get().unsubscribe(clientID);
                        } catch (RuntimeException e) {
                            throw e;
                        } catch (Exception e) {
                            log.error("[{}] [{}] Failed to unsubscribe the topic.", topic, clientID, e);
                            channel.close();
                        }
                    }
                }
            }).exceptionally(ex -> {
                log.error("[{}] [{}] Failed to get topic reference when process UNSUB.", topic, clientID, ex);
                channel.close();
                return null;
            });
        }

        // ack the client
        int messageID = msg.variableHeader().messageId();
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_LEAST_ONCE,
                false, 0);
        MqttUnsubAckMessage ackMessage = new MqttUnsubAckMessage(fixedHeader,
                MqttMessageIdVariableHeader.from(messageID));

        log.info("Sending UNSUBACK message {} to {}", ackMessage, clientID);
        channel.writeAndFlush(ackMessage);
    }

    @Override
    public void notifyChannelWritable(Channel channel) {
        if (log.isDebugEnabled()) {
            log.debug("[Notify Channel Writable] [{}]", channel);
        }
        channel.flush();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        serverCnx = new MQTTServerCnx(pulsarService, ctx);
    }

    private void initializeKeepAliveTimeout(Channel channel, MqttConnectMessage msg, final String clientId) {
        int keepAlive = msg.variableHeader().keepAliveTimeSeconds();
        log.info("Configuring connection. CId={}", clientId);
        NettyUtils.keepAlive(channel, keepAlive);
        NettyUtils.cleanSession(channel, msg.variableHeader().isCleanSession());
        NettyUtils.clientID(channel, clientId);
        int idleTime = Math.round(keepAlive * 1.5f);
        setIdleTime(channel.pipeline(), idleTime);
        log.debug("The connection has been configured CId={}, keepAlive={}, cleanSession={}, idleTime={}",
                clientId, keepAlive, msg.variableHeader().isCleanSession(), idleTime);
    }

    private void setIdleTime(ChannelPipeline pipeline, int idleTime) {
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(idleTime, 0, 0));
    }

    private MqttConnAckMessage connAck(MqttConnectReturnCode returnCode) {
        return connAck(returnCode, false);
    }

    private MqttConnAckMessage connAckWithSessionPresent(MqttConnectReturnCode returnCode) {
        return connAck(returnCode, true);
    }

    private MqttConnAckMessage connAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(returnCode, sessionPresent);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    private boolean sendAck(ConnectionDescriptor descriptor, MqttConnectReturnCode returnCode, final String clientId) {
        log.info("Sending connect ACK. CId={}", clientId);
        final boolean success = descriptor.assignState(DISCONNECTED, SENDACK);
        if (!success) {
            return false;
        }

        MqttConnAckMessage okResp = connAck(returnCode);

        descriptor.writeAndFlush(okResp);
        log.info("The connect ACK has been sent. CId={}", clientId);
        return true;
    }

    private boolean removeSubscriptions(ConnectionDescriptor descriptor, String clientID) {
        if (descriptor != null) {
            final boolean success = descriptor.assignState(ESTABLISHED, SUBSCRIPTIONS_REMOVED);
            if (!success) {
                return false;
            }
        }
        // todo remove subscriptions from Pulsar.
        return true;
    }

    private List<MqttTopicSubscription> doVerify(String clientID, String username, MqttSubscribeMessage msg) {
        List<MqttTopicSubscription> ackTopics = new ArrayList<>();

        for (MqttTopicSubscription req : msg.payload().topicSubscriptions()) {
            MqttQoS qos = req.qualityOfService();
            ackTopics.add(new MqttTopicSubscription(req.topicName(), qos));
        }
        return ackTopics;
    }

    private MqttSubAckMessage doAckMessageFromValidateFilters(List<MqttTopicSubscription> topicFilters, int messageId) {
        List<Integer> grantedQoSLevels = new ArrayList<>();
        for (MqttTopicSubscription req : topicFilters) {
            grantedQoSLevels.add(req.qualityOfService().value());
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQoSLevels);
        return new MqttSubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(messageId), payload);
    }
}
