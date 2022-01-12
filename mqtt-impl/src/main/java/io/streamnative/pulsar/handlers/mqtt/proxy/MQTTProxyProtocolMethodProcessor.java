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
import io.netty.channel.ChannelException;
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
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.support.AbstractCommonProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
import io.streamnative.pulsar.handlers.mqtt.utils.ExceptionUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
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
    private final ConcurrentHashMap<Integer, AtomicInteger> subscribeTopicsCount;
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
        this.subscribeTopicsCount = new ConcurrentHashMap<>();
    }

    @Override
    public void doProcessConnect(MqttConnectMessage msg, String userRole) {
        connection = Connection.builder()
                .protocolVersion(msg.variableHeader().version())
                .clientId(msg.payload().clientIdentifier())
                .userRole(userRole)
                .cleanSession(msg.variableHeader().isCleanSession())
                .connectMessage(msg)
                .keepAliveTime(msg.variableHeader().keepAliveTimeSeconds())
                .channel(channel)
                .connectionManager(connectionManager)
                .serverReceivePubMaximum(proxyConfig.getReceiveMaximum())
                .build();
        connection.sendConnAck();
    }

    @Override
    public void processPublish(MqttPublishMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] publish to topic = {}, CId={}",
                    msg.variableHeader().topicName(), connection.getClientId());
        }
        final int packetId = msg.variableHeader().packetId();
        final String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
        CompletableFuture<InetSocketAddress> lookupResult = lookupHandler.findBroker(
                TopicName.get(pulsarTopicName));
        lookupResult
                .thenCompose(brokerAddress -> writeToBroker(brokerAddress, pulsarTopicName, msg))
                .exceptionally(ex -> {
                    msg.release();
                    log.error("[Proxy Publish] Failed to publish for topic : {}, CId : {}",
                            msg.variableHeader().topicName(), connection.getClientId(), ex);
                    MopExceptionHelper.handle(MqttMessageType.PUBLISH, packetId, channel, ex);
                    return null;
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
            connection.close()
                    .thenAccept(__-> connectionManager.removeConnection(connection));
        }
    }

    @Override
    public void processConnectionLost() {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Connection Lost] [{}] ", connection.getClientId());
        }
        final Connection connection = NettyUtils.getConnection(channel);
        connectionManager.removeConnection(connection);
        brokerPool.forEach((k, v) -> v.close());
        brokerPool.clear();
        topicBrokers.clear();
    }

    @Override
    public void processSubscribe(MqttSubscribeMessage msg) {
        final String clientId = connection.getClientId();
        AckHandler ackHandler = connection.getAckHandler();
        int packetId = msg.variableHeader().messageId();
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Subscribe] [{}] msg: {}", clientId, msg);
        }
        PulsarTopicUtils.asyncGetTopicsForSubscribeMsg(msg, proxyConfig.getDefaultTenant(),
                        proxyConfig.getDefaultNamespace(), pulsarService, proxyConfig.getDefaultTopicDomain())
                .thenCompose(topics -> {
                    if (CollectionUtils.isEmpty(topics)) {
                        throw new RuntimeException(String.format("Client %s can not found topics %s",
                                clientId, msg.payload().topicSubscriptions()));
                    }
                    List<CompletableFuture<Void>> writeToBrokerFuture =
                            topics.stream().map(topic -> lookupHandler.findBroker(TopicName.get(topic))
                                            .thenCompose(brokerAddress -> writeToBroker(brokerAddress, topic, msg))
                                            .thenAccept(__ -> increaseSubscribeTopicsCount(
                                                    msg.variableHeader().messageId(), 1)))
                                    .collect(Collectors.toList());
                    return FutureUtil.waitForAll(writeToBrokerFuture);
                })
                .exceptionally(ex -> {
                    Throwable causeIfExist = ExceptionUtils.getCauseIfExist(ex);
                    log.error("[Proxy Subscribe] Failed to process subscribe for {}", clientId, causeIfExist);
                    SubscribeAck subscribeAck = SubscribeAck
                            .builder()
                            .isSuccess(false)
                            .packetId(packetId)
                            .errorReason(MqttSubAckMessageHelper.ErrorReason.UNSPECIFIED_ERROR)
                            .reasonStr("[ MOP ERROR ]" + causeIfExist.getMessage())
                            .build();
                    ackHandler.sendSubscribeAck(connection, subscribeAck)
                            .addListener(__ -> subscribeTopicsCount.remove(packetId));
                    return null;
                });
    }

    @Override
    public void processUnSubscribe(MqttUnsubscribeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy UnSubscribe] [{}]", connection.getClientId());
        }
        List<String> topics = msg.payload().topics();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String topic : topics) {
            CompletableFuture<InetSocketAddress> lookupResult = lookupHandler.findBroker(TopicName.get(topic));
            futures.add(
                    lookupResult.thenCompose(brokerAddress -> writeToBroker(brokerAddress, topic, msg)));
        }
        FutureUtil.waitForAll(futures)
                .exceptionally(ex -> {
                    log.error("[Proxy UnSubscribe] Failed to perform lookup request", ex);
                    channel.close();
                    return null;
        });
    }

    private CompletableFuture<Void> writeToBroker(InetSocketAddress mqttBroker, String topic, MqttMessage msg) {
        CompletableFuture<MQTTProxyExchanger> proxyExchanger = connectToBroker(mqttBroker, topic);
        return proxyExchanger.thenCompose(exchanger -> writeToBroker(exchanger, msg));
    }

    private CompletableFuture<Void> writeToBroker(MQTTProxyExchanger exchanger, MqttMessage msg) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (exchanger.isWritable()) {
            exchanger.writeAndFlush(msg).addListener(future -> {
                if (future.isSuccess()) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(future.cause());
                }
            });
        } else {
            log.error("The broker channel({}) is not writable!", exchanger.getMqttBroker());
            channel.close();
            exchanger.close();
            result.completeExceptionally(new ChannelException("Broker channel : {} is not writable"
                    + exchanger.getBrokerChannel()));
        }
        return result;
    }

    private CompletableFuture<MQTTProxyExchanger> connectToBroker(InetSocketAddress mqttBroker, String topic) {
        return topicBrokers.computeIfAbsent(topic, key -> {
            CompletableFuture<MQTTProxyExchanger> future = new CompletableFuture<>();
            try {
                MQTTProxyExchanger result = brokerPool.computeIfAbsent(mqttBroker, addr ->
                        new MQTTProxyExchanger(this, mqttBroker, proxyConfig.getMqttMessageMaxLength()));
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

    /**
     *  MQTT support subscribe many topics in one subscribe request.
     *  We need to record it's subscribe count.
     * @param seq
     * @param count
     */
    private void increaseSubscribeTopicsCount(int seq, int count) {
        AtomicInteger subscribeCount = subscribeTopicsCount.putIfAbsent(seq, new AtomicInteger(count));
        if (subscribeCount != null) {
            subscribeCount.addAndGet(count);
        }
    }

    private int decreaseSubscribeTopicsCount(int seq) {
        AtomicInteger subscribeCount = subscribeTopicsCount.get(seq);
        if (subscribeCount == null) {
            log.warn("Unexpected subscribe behavior for the proxy, respond seq {} "
                    + "but but the seq does not tracked by the proxy. ", seq);
            return -1;
        } else {
            int value = subscribeCount.decrementAndGet();
            if (value == 0) {
                subscribeTopicsCount.remove(seq);
            }
            return value;
        }
    }

    /**
     * If one sub-ack succeed, we need to decrease it's sub-count.
     * As the sub-count return zero, it means the subscribe action succeed.
     * @param seq
     * @return
     */
    public boolean checkIfSendSubAck(int seq) {
        return decreaseSubscribeTopicsCount(seq) == 0;
    }
}
