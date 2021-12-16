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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.MopExceptionHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.support.AbstractCommonProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
/**
 * Proxy inbound handler is the bridge between proxy and MoP.
 */
@Slf4j
public class MQTTProxyProtocolMethodProcessor extends AbstractCommonProtocolMethodProcessor {

    @Getter
    private Connection connection;
    private final LookupHandler lookupHandler;
    private final MQTTProxyConfiguration proxyConfig;
    private final PulsarService pulsarService;
    private final Map<String, CompletableFuture<MQTTProxyExchanger>> topicBrokers;
    private final Map<InetSocketAddress, MQTTProxyExchanger> brokerPool;
    // Map sequence Id -> topic count
    private final ConcurrentHashMap<Integer, AtomicInteger> topicCountForSequenceId;
    private final MQTTConnectionManager connectionManager;

    public MQTTProxyProtocolMethodProcessor(MQTTProxyService proxyService, ChannelHandlerContext ctx) {
        super(proxyService.getAuthenticationService(),
                proxyService.getProxyConfig().isMqttAuthenticationEnabled(), ctx);
        this.pulsarService = proxyService.getPulsarService();
        this.lookupHandler = proxyService.getLookupHandler();
        this.proxyConfig = proxyService.getProxyConfig();
        this.connectionManager = proxyService.getConnectionManager();
        this.topicBrokers = new ConcurrentHashMap<>();
        this.brokerPool = new ConcurrentHashMap<>();
        this.topicCountForSequenceId = new ConcurrentHashMap<>();
    }

    @Override
    public void initConnection(MqttConnectMessage msg, String clientId, String userRole, AckHandler ackHandler) {
        this.connection =  Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(clientId)
                .userRole(userRole)
                .cleanSession(msg.variableHeader().isCleanSession())
                .connectMessage(msg)
                .keepAliveTime(msg.variableHeader().keepAliveTimeSeconds())
                .channel(channel)
                .connectionManager(connectionManager)
                .serverReceivePubMaximum(proxyConfig.getReceiveMaximum())
                .ackHandler(ackHandler)
                .build();
        connection.ackHandler().sendConnAck();
    }

    @Override
    public void processPublish(MqttPublishMessage msg) {
        final int packetId = msg.variableHeader().packetId();
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] publish to topic = {}, CId={}",
                    msg.variableHeader().topicName(), connection.getClientId());
        }
        String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
        CompletableFuture<InetSocketAddress> lookupResult = lookupHandler.findBroker(
                TopicName.get(pulsarTopicName));
        lookupResult.whenComplete((brokerAddress, throwable) -> {
            if (null != throwable) {
                log.error("[Proxy Publish] Failed to perform lookup request for topic : {}, CId : {}",
                        msg.variableHeader().topicName(), connection.getClientId(), throwable);
                MopExceptionHelper.handle(MqttMessageType.PUBLISH, packetId, channel, throwable);
                return;
            }
            writeToMqttBroker(channel, msg, pulsarTopicName, brokerAddress);
        });
    }

    @Override
    public void processPingReq() {
        channel.writeAndFlush(pingResp());
        brokerPool.forEach((k, v) -> v.writeAndFlush(pingReq()));
    }

    @Override
    public void processDisconnect(MqttMessage msg) {
        String clientId = connection.getClientId();
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
    public void processConnectionLost() {
        String clientId = connection.getClientId();
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
    public void processSubscribe(MqttSubscribeMessage msg) {
        String clientId = connection.getClientId();
        int protocolVersion = connection.getProtocolVersion();
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
    public void processUnSubscribe(MqttUnsubscribeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy UnSubscribe] [{}]", connection.getClientId());
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
                        connection.getClientId(), mqttBroker, error);
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

    public Channel getChannel() {
        return this.channel;
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
