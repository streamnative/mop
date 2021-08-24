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

import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.ESTABLISHED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.SENDACK;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.SUBSCRIPTIONS_REMOVED;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor;
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptorStore;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.utils.AuthUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.naming.AuthenticationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Proxy inbound handler is the bridge between proxy and MoP.
 */
@Slf4j
public class ProxyInboundHandler implements ProtocolMethodProcessor {
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    private Map<String, ProxyHandler> proxyHandlerMap;
    private ProxyHandler proxyHandler;
    private LookupHandler lookupHandler;
    private final ProxyConfiguration proxyConfig;
    private final PulsarService pulsarService;

    private List<Object> connectMsgList = new ArrayList<>();

    public ProxyInboundHandler(ProxyService proxyService, ProxyConnection proxyConnection,
           ProxyConfiguration proxyConfig) {
        log.info("ProxyConnection init ...");
        this.proxyService = proxyService;
        this.pulsarService = proxyService.getPulsarService();
        this.proxyConnection = proxyConnection;
        lookupHandler = proxyService.getLookupHandler();
        this.proxyHandlerMap = new ConcurrentHashMap<>();
        this.proxyConfig = proxyConfig;
    }

    // client -> proxy
    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        log.info("process CONNECT message. CId={}, username={}", clientId, payload.userName());

        // Client must specify the client ID except enable clean session on the connection.
        if (clientId == null || clientId.length() == 0) {
            if (!msg.variableHeader().isCleanSession()) {
                MqttConnAckMessage badId = connAck(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false);

                channel.writeAndFlush(badId);
                channel.close();
                log.error("The MQTT client ID cannot be empty. Username={}", payload.userName());
                return;
            }

            // Generating client id.
            clientId = UUID.randomUUID().toString().replace("-", "");
            log.info("Client has connected with a server generated identifier. CId={}, username={}", clientId,
                    payload.userName());
        }

        // Authenticate the client
        if (!proxyService.getProxyConfig().isMqttAuthenticationEnabled()) {
            log.info("Authentication is disabled, allowing client. CId={}, username={}", clientId, payload.userName());
        } else {
            boolean authenticated = false;
            for (Map.Entry<String, AuthenticationProvider> entry : proxyService.getAuthProviders().entrySet()) {
                try {
                    entry.getValue().authenticate(AuthUtils.getAuthData(entry.getKey(), payload));
                    authenticated = true;
                    break;
                } catch (AuthenticationException e) {
                    log.info("Authentication failed with method: {}. CId={}, username={}",
                             entry.getKey(), clientId, payload.userName());
                }
            }
            if (!authenticated) {
                channel.writeAndFlush(
                    connAck(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false));
                channel.close();
                log.error("Invalid or incorrect authentication. CId={}, username={}", clientId, payload.userName());
                return;
            }
        }

        NettyUtils.clientID(channel, clientId);

        connectMsgList.add(msg);
        ConnectionDescriptor descriptor = new ConnectionDescriptor(clientId, channel,
                msg.variableHeader().isCleanSession());

        if (!sendAck(descriptor, msg, clientId)) {
            channel.close();
        }
    }

    @Override
    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
        log.info("processPubAck...");
    }

    // proxy -> MoP
    @Override
    public void processPublish(Channel channel, MqttPublishMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] [{}] handle processPublish", msg.variableHeader().topicName());
        }
        CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();
        try {
            lookupResult = lookupHandler.findBroker(
                    TopicName.get(PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                            proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace())), "mqtt");
        } catch (Exception e) {
            log.error("[Proxy Publish] Failed to perform lookup request for topic {}",
                    msg.variableHeader().topicName(), e);
            channel.close();
        }

        lookupResult.whenComplete((pair, throwable) -> {
            if (null != throwable) {
                log.error("[Proxy Publish] Failed to perform lookup request for topic {}",
                        msg.variableHeader().topicName(), throwable);
                channel.close();
                return;
            }
            final String topicName;
            try {
                topicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                        proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace());
            } catch (Exception e) {
                log.error("[Proxy Publish] Failed to get Pulsar topic name for topic {}",
                        msg.variableHeader().topicName(), e);
                channel.close();
                return;
            }

            writeAndFlush(topicName, pair.getLeft(), pair.getRight(), channel, msg);
        });
    }

    @Override
    public void processPubRel(Channel channel, MqttMessage msg) {
        log.info("processPubRel...");
    }

    @Override
    public void processPubRec(Channel channel, MqttMessage msg) {
        log.info("processPubRec...");
    }

    @Override
    public void processPubComp(Channel channel, MqttMessage msg) {
        log.info("processPubComp...");
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

        // clear the proxyHandlerMap, when the cnx is disconnected
        proxyHandlerMap.clear();
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
        log.info("[Proxy Subscribe] [{}] msg: {}", channel, msg);
        CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicsForSubscribeMsg(msg,
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(), pulsarService);

        if (topicListFuture == null) {
            channel.close();
            return;
        }

        topicListFuture.thenCompose(topics -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String t : topics) {
                CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();
                try {
                    lookupResult = lookupHandler.findBroker(TopicName.get(t), "mqtt");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                futures.add(lookupResult.thenAccept(pair -> {
                    proxyConnection.increaseSubscribeTopicsCount(msg.variableHeader().messageId(), 1);
                    writeAndFlush(t, pair.getLeft(), pair.getRight(),
                            channel, msg);
                }));
            }
            return FutureUtil.waitForAll(futures);
        }).exceptionally(ex -> {
            log.error("[Proxy Subscribe] Failed to process subscribe for {}", channel, ex);
            channel.close();
            return null;
        });
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        log.info("processUnSubscribe...");

        List<String> topics = msg.payload().topics();
        for (String topic : topics) {
            CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();
            try {
                lookupResult = lookupHandler.findBroker(TopicName.get(topic), "mqtt");
            } catch (Exception e) {
                log.error("[Proxy UnSubscribe] Failed to perform lookup request", e);
                channel.close();
            }

            lookupResult.whenComplete((pair, throwable) -> {
                if (null != throwable) {
                    log.error("[Proxy UnSubscribe] Failed to perform lookup request", throwable);
                    channel.close();
                    return;
                }

                writeAndFlush(topic, pair.getLeft(), pair.getRight(), channel, msg);
            });
        }
    }

    @Override
    public void notifyChannelWritable(Channel channel) {
        log.info("notifyChannelWritable...");
        channel.flush();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("channelActive...");
    }

    private MqttConnAckMessage connAck(MqttConnectReturnCode returnCode, boolean sessionPresent) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(returnCode, sessionPresent);
        return new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
    }

    private boolean sendAck(ConnectionDescriptor descriptor, MqttConnectMessage msg, final String clientId) {
        log.info("Sending connect ACK. CId={}", clientId);
        final boolean success = descriptor.assignState(DISCONNECTED, SENDACK);
        if (!success) {
            return false;
        }

        MqttConnAckMessage okResp = connAck(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);

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

    private ProxyHandler getProxyHandler(String topic, String mqttBrokerHost, int mqttBrokerPort) {
        return proxyHandlerMap.computeIfAbsent(topic, key -> {
            try {
                return new ProxyHandler(proxyService,
                        proxyConnection,
                        mqttBrokerHost,
                        mqttBrokerPort,
                        connectMsgList);
            } catch (Exception e) {
                log.error("[Proxy UnSubscribe] Failed to perform lookup request", e);
                return null;
            }
        });
    }

    private void writeAndFlush(String topic, String mqttBrokerHost, int mqttBrokerPort,
                               Channel channel, MqttMessage msg) {
        ProxyHandler proxyHandler = getProxyHandler(topic, mqttBrokerHost, mqttBrokerPort);
        if (null == proxyHandler) {
            channel.close();
            return;
        }
        proxyHandler.brokerFuture().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("[{}] MoP proxy failed to connect with MoP broker({}:{}).",
                        msg.fixedHeader().messageType(), mqttBrokerHost, mqttBrokerPort, throwable);
                channel.close();
                return;
            }
            if (proxyHandler.getBrokerChannel().isWritable()) {
                proxyHandler.getBrokerChannel().writeAndFlush(msg);
            } else {
                log.error("The broker channel({}:{}) is not writable!", mqttBrokerHost, mqttBrokerPort);
                channel.close();
                proxyHandler.close();
            }
        });
    }

}
