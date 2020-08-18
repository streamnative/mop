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
import io.netty.handler.codec.mqtt.*;
import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;

import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.SENDACK;

@Slf4j
public class ProxyInboundHandler implements ProtocolMethodProcessor {
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    private ProxyConfiguration proxyConfig;
    @Getter
    private ChannelHandlerContext cnx;
    // topic : ProxyHandler
    private Map<String, ProxyHandler> proxyHandler;

    private LookupHandler lookupHandler;

    private List<Object> connectMsgList = new ArrayList<>();

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

    public ProxyInboundHandler(ProxyService proxyService, ProxyConnection proxyConnection) throws PulsarClientException {
        log.info("ProxyConnection init ...");
        this.proxyService = proxyService;
        this.proxyConfig = proxyService.getProxyConfig();
        this.proxyConnection = proxyConnection;
        lookupHandler = proxyService.getLookupHandler();
    }

    // client -> proxy
    @Override
    public void processConnect(Channel channel, MqttConnectMessage msg) {
        log.info("processConnect...");
        MqttConnectPayload payload = msg.payload();
        String clientId = payload.clientIdentifier();
        log.info("process CONNECT message. CId={}, username={}", clientId, payload.userName());

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
        log.info("processPublish...");
        CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();
        try {
            lookupResult = lookupHandler.findBroker(TopicName.get(msg.variableHeader().topicName()), "mop");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 1. need to cache (cnx pool)
        // 2. multi MoP, will have multi proxyHandler
        lookupResult.whenComplete((pair, throwable) -> {
            try {
                ProxyHandler proxyHandler = new ProxyHandler(proxyService, proxyConnection,pair.getLeft(),pair.getRight(), connectMsgList);
                proxyHandler.getBrokerChannel().writeAndFlush(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        log.info("processDisconnect...");
    }

    @Override
    public void processConnectionLost(String clientID, Channel channel) {
        log.info("processConnectionLost...");
    }

    @Override
    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        log.info("processSubscribe...");
    }

    @Override
    public void processUnSubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        log.info("processUnSubscribe...");
    }

    @Override
    public void notifyChannelWritable(Channel channel) {
        log.info("notifyChannelWritable...");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("channelActive...");
    }

}
