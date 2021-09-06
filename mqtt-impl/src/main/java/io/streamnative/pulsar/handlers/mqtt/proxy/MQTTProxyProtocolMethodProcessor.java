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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.utils.AuthUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.AuthenticationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
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

    private final Map<String, MQTTProxyExchanger> proxyExchangerMap;
    private final List<MqttConnectMessage> connectMsgList;
    // Map sequence Id -> topic count
    private final ConcurrentHashMap<Integer, AtomicInteger> topicCountForSequenceId;

    public MQTTProxyProtocolMethodProcessor(MQTTProxyService proxyService, MQTTProxyHandler proxyHandler) {
        this.proxyService = proxyService;
        this.proxyHandler = proxyHandler;
        this.pulsarService = proxyService.getPulsarService();
        this.lookupHandler = proxyService.getLookupHandler();
        this.proxyConfig = proxyService.getProxyConfig();
        this.proxyExchangerMap = new ConcurrentHashMap<>();
        this.connectMsgList = new ArrayList<>();
        this.topicCountForSequenceId = new ConcurrentHashMap<>();
    }

    // client -> proxy
    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        MqttConnectMessage connectMessage = msg;
        MqttConnectPayload payload = connectMessage.payload();
        String clientId = payload.clientIdentifier();
        if (log.isDebugEnabled()) {
            log.debug("process CONNECT message. CId={}, username={}", clientId, payload.userName());
        }
        // Client must specify the client ID except enable clean session on the connection.
        if (StringUtils.isEmpty(clientId)) {
            // Generating client id.
            clientId = MqttMessageUtils.createClientIdentifier(channel);
            connectMessage = MqttMessageUtils.createMqttConnectMessage(msg, clientId);
            if (log.isDebugEnabled()) {
                log.debug("Client has connected with generated identifier. CId={}", clientId);
            }
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
                        MqttMessageUtils.connAck(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD));
                channel.close();
                log.error("Invalid or incorrect authentication. CId={}, username={}", clientId, payload.userName());
                return;
            }
        }

        NettyUtils.attachClientID(channel, clientId);
        connectMsgList.add(connectMessage);
        MqttConnAckMessage ackMessage = MqttMessageUtils.connAck(MqttConnectReturnCode.CONNECTION_ACCEPTED);
        channel.writeAndFlush(ackMessage);
    }

    @Override
    public void processPubAck(Channel channel, MqttPubAckMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("processPubAck...");
        }
    }

    // proxy -> MoP
    @Override
    public void processPublish(Channel channel, MqttPublishMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Publish] [{}] handle processPublish", msg.variableHeader().topicName());
        }
        String pulsarTopicName = PulsarTopicUtils.getEncodedPulsarTopicName(msg.variableHeader().topicName(),
                proxyConfig.getDefaultTenant(), proxyConfig.getDefaultNamespace(),
                TopicDomain.getEnum(proxyConfig.getDefaultTopicDomain()));
        CompletableFuture<Pair<String, Integer>> lookupResult = lookupHandler.findBroker(
                TopicName.get(pulsarTopicName));
        lookupResult.whenComplete((pair, throwable) -> {
            if (null != throwable) {
                log.error("[Proxy Publish] Failed to perform lookup request for topic {}",
                        msg.variableHeader().topicName(), throwable);
                channel.close();
                return;
            }
            writeAndFlush(pulsarTopicName, pair, channel, msg);
        });
    }

    @Override
    public void processPubRel(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("processPubRel...");
        }
    }

    @Override
    public void processPubRec(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("processPubRec...");
        }
    }

    @Override
    public void processPubComp(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("processPubComp...");
        }
    }

    @Override
    public void processDisconnect(Channel channel, MqttMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Disconnect] [{}] ", channel);
        }
        channel.close();
        proxyExchangerMap.forEach((k, v) -> v.getBrokerChannel().writeAndFlush(msg));
        proxyExchangerMap.forEach((k, v) -> v.close());
        proxyExchangerMap.clear();
    }

    @Override
    public void processConnectionLost(Channel channel) {
        if (log.isDebugEnabled()) {
            log.debug("[Connection Lost] channel [{}] ", channel);
        }
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("[Proxy Subscribe] [{}] msg: {}", channel, msg);
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
            for (String t : topics) {
                CompletableFuture<Pair<String, Integer>> lookupResult = lookupHandler.findBroker(TopicName.get(t));
                futures.add(lookupResult.thenAccept(pair -> {
                    increaseSubscribeTopicsCount(msg.variableHeader().messageId(), 1);
                    writeAndFlush(t, pair, channel, msg);
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
        if (log.isDebugEnabled()) {
            log.debug("processUnSubscribe...");
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

                writeAndFlush(topic, pair, channel, msg);
            });
        }
    }

    @Override
    public void notifyChannelWritable(Channel channel) {
        if (log.isDebugEnabled()) {
            log.debug("notifyChannelWritable...");
        }
        channel.flush();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("channelActive...");
        }
    }

    private MQTTProxyExchanger getProxyExchanger(String topic, Pair<String, Integer> mqttBrokerHostAndPort) {
        return proxyExchangerMap.computeIfAbsent(topic, key -> {
            try {
                return new MQTTProxyExchanger(
                        this,
                        mqttBrokerHostAndPort,
                        connectMsgList);
            } catch (Exception e) {
                log.error("[Proxy UnSubscribe] Failed to perform lookup request", e);
                return null;
            }
        });
    }

    private void writeAndFlush(String topic, Pair<String, Integer> mqttBrokerHostAndPort,
                               Channel channel, MqttMessage msg) {
        MQTTProxyExchanger proxyExchanger = getProxyExchanger(topic, mqttBrokerHostAndPort);
        if (null == proxyExchanger) {
            log.error("proxy handler is null for topic : {}, mqttBrokerHostAndPort : {}, closing channel",
                    topic, mqttBrokerHostAndPort);
            channel.close();
            return;
        }
        proxyExchanger.brokerFuture().whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("[{}] MoP proxy failed to connect with MoP broker({}).",
                        msg.fixedHeader().messageType(), mqttBrokerHostAndPort, throwable);
                channel.close();
                return;
            }
            if (proxyExchanger.getBrokerChannel().isWritable()) {
                proxyExchanger.getBrokerChannel().writeAndFlush(msg);
            } else {
                log.error("The broker channel({}) is not writable!", mqttBrokerHostAndPort);
                channel.close();
                proxyExchanger.close();
            }
        });
    }

    public Channel clientChannel() {
        return proxyHandler.getCnx().channel();
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
