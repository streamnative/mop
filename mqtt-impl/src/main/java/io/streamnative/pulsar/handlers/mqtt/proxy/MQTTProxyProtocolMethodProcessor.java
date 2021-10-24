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
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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

    private final Map<String, CompletableFuture<MQTTProxyExchanger>> proxyExchangerMap;
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
        this.proxyExchangerMap = new ConcurrentHashMap<>();
        this.topicCountForSequenceId = new ConcurrentHashMap<>();
    }

    // client -> proxy
    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
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
                MqttConnAckMessage connAck = MqttMessageUtils.
                        connAck(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
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

        Connection connection = new Connection(clientId, channel, msg.variableHeader().isCleanSession());
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
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] publish to topic = {}, CId={}",
                    msg.variableHeader().topicName(), NettyUtils.getClientId(channel));
        }
        String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
        CompletableFuture<Pair<String, Integer>> lookupResult = lookupHandler.findBroker(
                TopicName.get(pulsarTopicName));
        lookupResult.whenComplete((pair, throwable) -> {
            if (null != throwable) {
                log.error("[Proxy Publish] Failed to perform lookup request for topic : {}, CId : {}",
                        msg.variableHeader().topicName(), NettyUtils.getClientId(channel), throwable);
                channel.close();
                return;
            }
            writeToMqttBroker(channel, msg, pulsarTopicName, pair);
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
        proxyExchangerMap.forEach((k, v) -> v.whenComplete((exchanger, error) -> {
            exchanger.writeAndFlush(pingReq());
        }));
    }

    @Override
    public void processDisconnect(Channel channel, MqttMessage msg) {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Disconnect] [{}] ", clientId);
        }
        proxyExchangerMap.forEach((k, v) -> v.whenComplete((exchanger, error) -> {
            exchanger.writeAndFlush(msg);
            exchanger.close();
        }));
        proxyExchangerMap.clear();

        Connection connection = NettyUtils.getConnection(channel);
        boolean success = connectionManager.removeConnection(connection);
        if (success) {
            connection.close();
        } else {
            log.warn("connection is null. close CId={}", clientId);
            channel.close();
        }
        if (log.isDebugEnabled()) {
            log.debug("The DISCONNECT message has been processed. CId={}", clientId);
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
        proxyExchangerMap.forEach((k, v) -> v.whenComplete((exchanger, error) -> {
            exchanger.close();
        }));
        proxyExchangerMap.clear();
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientId = NettyUtils.getClientId(channel);
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Subscribe] [{}] msg: {}", clientId, msg);
        }
        CompletableFuture<List<String>> topicListFuture = PulsarTopicUtils.asyncGetTopicsForSubscribeMsg(msg,
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(), pulsarService,
                proxyConfig.getDefaultTopicDomain());

        if (topicListFuture == null) {
            channel.close();
            return;
        }

        topicListFuture.thenCompose(topics -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String topic : topics) {
                CompletableFuture<Pair<String, Integer>> lookupResult = lookupHandler.findBroker(TopicName.get(topic));
                futures.add(lookupResult.thenAccept(pair -> {
                    increaseSubscribeTopicsCount(msg.variableHeader().messageId(), 1);
                    writeToMqttBroker(channel, msg, topic, pair);
                }));
            }
            return FutureUtil.waitForAll(futures);
        }).exceptionally(ex -> {
            log.error("[Proxy Subscribe] Failed to process subscribe for {}", clientId, ex);
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
            CompletableFuture<Pair<String, Integer>> lookupResult = lookupHandler.findBroker(TopicName.get(topic));
            lookupResult.whenComplete((pair, throwable) -> {
                if (null != throwable) {
                    log.error("[Proxy UnSubscribe] Failed to perform lookup request", throwable);
                    channel.close();
                    return;
                }
                writeToMqttBroker(channel, msg, topic, pair);
            });
        }
    }

    private void writeToMqttBroker(Channel channel, MqttMessage msg, String topic, Pair<String, Integer> pair) {
        CompletableFuture<MQTTProxyExchanger> proxyExchanger = createProxyExchanger(topic, pair);
        proxyExchanger.whenComplete((exchanger, error) -> {
            if (error != null) {
                log.error("[{}] [{}] MoP proxy failed to connect with MoP broker({}).",
                        NettyUtils.getClientId(channel), msg.fixedHeader().messageType(), pair, error);
                channel.close();
                return;
            }
            if (exchanger.isWritable()) {
                exchanger.writeAndFlush(msg);
            } else {
                log.error("The broker channel({}) is not writable!", pair);
                channel.close();
                exchanger.close();
            }
        });
    }

    private CompletableFuture<MQTTProxyExchanger> createProxyExchanger(String topic, Pair<String, Integer> mqttBroker) {
        return proxyExchangerMap.computeIfAbsent(topic, key -> {
            CompletableFuture<MQTTProxyExchanger> future = new CompletableFuture<>();
            try {
                MQTTProxyExchanger exchanger = new MQTTProxyExchanger(this, mqttBroker);
                exchanger.connectedAck().thenAccept(__ -> future.complete(exchanger));
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
