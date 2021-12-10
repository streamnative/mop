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

import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.pingReq;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.pingResp;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.MopExceptionHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
/**
 * Proxy inbound handler is the bridge between proxy and MoP.
 */
@Slf4j
public class MQTTProxyProtocolMethodProcessor implements ProtocolMethodProcessor {

    private final MQTTProxyService proxyService;
    private final MQTTProxyHandler proxyHandler;
    private final LookupHandler lookupHandler;
    private final MQTTProxyConfiguration proxyConfig;
    private final PulsarService pulsarService;
    private final MQTTAuthenticationService authenticationService;

    private final Map<String, CompletableFuture<MQTTProxyExchanger>> topicBrokers;
    private final Map<InetSocketAddress, MQTTProxyExchanger> brokerPool;
    // Map sequence Id -> topic count
    private final ConcurrentHashMap<Integer, AtomicInteger> topicCountForSequenceId;
    private final MQTTConnectionManager connectionManager;

    public MQTTProxyProtocolMethodProcessor(MQTTProxyService proxyService, MQTTProxyHandler proxyHandler) {
        this.proxyService = proxyService;
        this.proxyHandler = proxyHandler;
        this.pulsarService = proxyService.getPulsarService();
        this.authenticationService = proxyService.getAuthenticationService();
        this.lookupHandler = proxyService.getLookupHandler();
        this.proxyConfig = proxyService.getProxyConfig();
        this.connectionManager = proxyService.getConnectionManager();
        this.topicBrokers = new ConcurrentHashMap<>();
        this.brokerPool = new ConcurrentHashMap<>();
        this.topicCountForSequenceId = new ConcurrentHashMap<>();
    }

    // client -> proxy
    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        final int protocolVersion = msg.variableHeader().version();
        MqttConnectMessage connectMessage = msg;
        MqttConnectPayload payload = connectMessage.payload();
        String clientId = payload.clientIdentifier();
        if (log.isDebugEnabled()) {
            log.debug("Proxy CONNECT message. CId={}, username={}", clientId, payload.userName());
        }
        if (StringUtils.isEmpty(clientId)) {
            // Generating client id.
            clientId = MqttMessageUtils.createClientIdentifier(channel);
            connectMessage = MqttMessageUtils.createMqttConnectMessage(msg, clientId);
            if (log.isDebugEnabled()) {
                log.debug("Proxy client has connected with generated identifier. CId={}", clientId);
            }
        }
        // Authenticate the client
        if (!proxyService.getProxyConfig().isMqttAuthenticationEnabled()) {
            log.info("Proxy authentication is disabled, allowing client. CId={}, username={}",
                    clientId, payload.userName());
        } else {
            MQTTAuthenticationService.AuthenticationResult authResult = authenticationService.authenticate(payload);
            if (authResult.isFailed()) {
                MqttMessage connAck = MqttUtils.isMqtt5(protocolVersion)
                        ? MqttConnAckMessageHelper.createMqtt(Mqtt5ConnReasonCode.BAD_USERNAME_OR_PASSWORD) :
                        MqttConnAckMessageHelper.createMqtt(
                                Mqtt3ConnReasonCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                channel.writeAndFlush(connAck);
                channel.close();
                log.error("Invalid or incorrect authentication. CId={}, username={}", clientId, payload.userName());
                return;
            }
        }
        NettyUtils.setClientId(channel, clientId);
        NettyUtils.setCleanSession(channel, msg.variableHeader().isCleanSession());
        NettyUtils.setConnectMsg(channel, connectMessage);
        NettyUtils.setKeepAliveTime(channel, MqttMessageUtils.getKeepAliveTime(msg));
        NettyUtils.addIdleStateHandler(channel, MqttMessageUtils.getKeepAliveTime(msg));
        NettyUtils.setProtocolVersion(channel, protocolVersion);

        Connection.ConnectionBuilder connectionBuilder = Connection.builder()
                .protocolVersion(protocolVersion)
                .clientId(clientId)
                .channel(channel)
                .manager(connectionManager)
                .serverReceivePubMaximum(proxyConfig.getReceiveMaximum())
                .cleanSession(msg.variableHeader().isCleanSession());
        Connection connection = connectionBuilder.build();
        connectionManager.addConnection(connection);
        NettyUtils.setConnection(channel, connection);
        connection.sendConnAck();
    }

    @Override
    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy PubAck] [{}]", NettyUtils.getClientId(channel));
        }
    }

    // proxy -> MoP
    @Override
    public void processPublish(Channel channel, MqttPublishMessage msg) {
        final int packetId = msg.variableHeader().packetId();
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] publish to topic = {}, CId={}",
                    msg.variableHeader().topicName(), NettyUtils.getClientId(channel));
        }
        String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
        CompletableFuture<InetSocketAddress> lookupResult = lookupHandler.findBroker(
                TopicName.get(pulsarTopicName));
        lookupResult.whenComplete((brokerAddress, throwable) -> {
            if (null != throwable) {
                log.error("[Proxy Publish] Failed to perform lookup request for topic : {}, CId : {}",
                        msg.variableHeader().topicName(), NettyUtils.getClientId(channel), throwable);
                MopExceptionHelper.handle(MqttMessageType.PUBLISH, packetId, channel, throwable);
                return;
            }
            writeToMqttBroker(channel, msg, pulsarTopicName, brokerAddress);
        });
    }

    @Override
    public void processPubRel(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy PubRel] [{}]", NettyUtils.getClientId(channel));
        }
    }

    @Override
    public void processPubRec(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy PubRec] [{}]", NettyUtils.getClientId(channel));
        }
    }

    @Override
    public void processPubComp(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy PubComp] [{}]", NettyUtils.getClientId(channel));
        }
    }

    @Override
    public void processPingReq(Channel channel) {
        channel.writeAndFlush(pingResp());
        brokerPool.forEach((k, v) -> v.writeAndFlush(pingReq()));
    }

    @Override
    public void processDisconnect(Channel channel, MqttMessage msg) {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Disconnect] [{}] ", clientId);
        }
        brokerPool.forEach((k, v) -> {
            v.writeAndFlush(msg);
            v.close();
        });
        brokerPool.clear();
        topicBrokers.clear();
        // When login, checkState(msg) failed, connection is null.
        Connection connection = NettyUtils.getConnection(channel);
        if (connection == null) {
            log.warn("connection is null. close CId={}", clientId);
            channel.close();
        } else {
            connectionManager.removeConnection(connection);
            connection.close();
        }
    }

    @Override
    public void processConnectionLost(Channel channel) {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Connection Lost] [{}] ", clientId);
        }
        Connection connection = NettyUtils.getConnection(channel);
        connectionManager.removeConnection(connection);
        brokerPool.forEach((k, v) -> v.close());
        brokerPool.clear();
        topicBrokers.clear();
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientId = NettyUtils.getClientId(channel);
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Subscribe] [{}] msg: {}", clientId, msg);
        }
        CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicsForSubscribeMsg(msg,
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(), pulsarService,
                proxyConfig.getDefaultTopicDomain());

        if (topicListFuture == null) {
            int messageId = msg.variableHeader().messageId();
            MqttMessage subAckMessage = MqttUtils.isMqtt5(protocolVersion)
                    ? MqttSubAckMessageHelper.createMqtt5(messageId, Mqtt5SubReasonCode.UNSPECIFIED_ERROR,
                    String.format("Client %s can not found topics %s.", clientId, msg.payload().topicSubscriptions())) :
                    MqttSubAckMessageHelper.createMqtt(messageId, Mqtt3SubReasonCode.FAILURE);
            channel.writeAndFlush(subAckMessage);
            channel.close();
            return;
        }

        topicListFuture.thenCompose(topics -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String topic : topics) {
                CompletableFuture<InetSocketAddress> lookupResult = lookupHandler.findBroker(TopicName.get(topic));
                futures.add(lookupResult.thenAccept(brokerAddress -> {
                    increaseSubscribeTopicsCount(msg.variableHeader().messageId(), 1);
                    writeToMqttBroker(channel, msg, topic, brokerAddress);
                }));
            }
            return FutureUtil.waitForAll(futures);
        }).exceptionally(ex -> {
            log.error("[Proxy Subscribe] Failed to process subscribe for {}", clientId, ex);
            int messageId = msg.variableHeader().messageId();
            MqttMessage subAckMessage = MqttUtils.isMqtt5(protocolVersion) ? MqttSubAckMessageHelper.createMqtt5(
                    messageId, Mqtt5SubReasonCode.UNSPECIFIED_ERROR, ex.getCause().getMessage()) :
                    MqttSubAckMessageHelper.createMqtt(messageId, Mqtt3SubReasonCode.FAILURE);
            channel.writeAndFlush(subAckMessage);
            channel.close();
            return null;
        });
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy UnSubscribe] [{}]", NettyUtils.getClientId(channel));
        }
        List<String> topics = msg.payload().topics();
        for (String topic : topics) {
            CompletableFuture<InetSocketAddress> lookupResult = lookupHandler.findBroker(TopicName.get(topic));
            lookupResult.whenComplete((brokerAddress, throwable) -> {
                if (null != throwable) {
                    log.error("[Proxy UnSubscribe] Failed to perform lookup request", throwable);
                    channel.close();
                    return;
                }
                writeToMqttBroker(channel, msg, topic, brokerAddress);
            });
        }
    }

    private void writeToMqttBroker(Channel channel, MqttMessage msg, String topic, InetSocketAddress mqttBroker) {
        CompletableFuture<MQTTProxyExchanger> proxyExchanger = createProxyExchanger(topic, mqttBroker);
        proxyExchanger.whenComplete((exchanger, error) -> {
            if (error != null) {
                log.error("[{}]] MoP proxy failed to connect with MoP broker({}).",
                        NettyUtils.getClientId(channel), mqttBroker, error);
                channel.close();
                return;
            }
            if (exchanger.isWritable()) {
                exchanger.writeAndFlush(msg);
            } else {
                log.error("The broker channel({}) is not writable!", mqttBroker);
                channel.close();
                exchanger.close();
            }
        });
    }

    private CompletableFuture<MQTTProxyExchanger> createProxyExchanger(String topic, InetSocketAddress mqttBroker) {
        return topicBrokers.computeIfAbsent(topic, key -> {
            CompletableFuture<MQTTProxyExchanger> future = new CompletableFuture<>();
            try {
                MQTTProxyExchanger result = brokerPool.computeIfAbsent(mqttBroker, addr ->
                        new MQTTProxyExchanger(this, mqttBroker));
                result.connectedAck().thenAccept(__ -> future.complete(result));
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }
            return future;
        });
    }

    public Channel clientChannel() {
        return proxyHandler.getCtx().channel();
    }

    public boolean increaseSubscribeTopicsCount(int seq, int count) {
        return topicCountForSequenceId.putIfAbsent(seq, new AtomicInteger(count)) == null;
    }

    public int decreaseSubscribeTopicsCount(int seq) {
        if (topicCountForSequenceId.get(seq) == null) {
            log.warn("Unexpected subscribe behavior for the proxy, respond seq {} "
                    + "but but the seq does not tracked by the proxy. ", seq);
            return -1;
        } else {
            int value = topicCountForSequenceId.get(seq).decrementAndGet();
            if (value == 0) {
                topicCountForSequenceId.remove(seq);
            }
            return value;
        }
    }
}
