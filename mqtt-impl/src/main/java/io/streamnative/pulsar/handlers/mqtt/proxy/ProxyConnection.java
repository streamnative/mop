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

import static com.google.common.base.Preconditions.checkState;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.streamnative.pulsar.handlers.mqtt.ProtocolMethodProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Proxy connection.
 */
@Slf4j
public class ProxyConnection extends ChannelInboundHandlerAdapter{
    private final ProtocolMethodProcessor processor;
    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig;
    @Getter
    private ChannelHandlerContext cnx;
    private State state;
    private ProxyHandler proxyHandler;

    private LookupHandler lookupHandler;

    private List<Object> connectMsgList = new ArrayList<>();
    // Map sequence Id -> topic count
    private ConcurrentHashMap<Integer, AtomicInteger> topicCountForSequenceId = new ConcurrentHashMap<>();

    private enum State {
        Init,
        RedirectLookup,
        RedirectToBroker,
        Closed
    }

    public ProxyConnection(ProxyService proxyService) throws PulsarClientException {
        log.info("ProxyConnection init ...");
        this.proxyService = proxyService;
        this.proxyConfig = proxyService.getProxyConfig();
        lookupHandler = proxyService.getLookupHandler();
        processor = new ProxyInboundHandler(proxyService, this, proxyConfig);
        state = State.Init;
    }

    @Override
    public void channelActive(ChannelHandlerContext cnx) throws Exception {
        super.channelActive(cnx);
        this.cnx = cnx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        this.close();
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        MqttMessage msg = (MqttMessage) message;
        MqttMessageType messageType = msg.fixedHeader().messageType();

        if (log.isDebugEnabled()) {
            log.info("Processing MQTT message, type={}", messageType);
        }
        try {
            switch (messageType) {
                case CONNECT:
                    checkState(msg instanceof MqttConnectMessage);
                    processor.processConnect(ctx.channel(), (MqttConnectMessage) msg);
                    break;
                case SUBSCRIBE:
                    checkState(msg instanceof MqttSubscribeMessage);
                    processor.processSubscribe(ctx.channel(), (MqttSubscribeMessage) msg);
                    break;
                case UNSUBSCRIBE:
                    checkState(msg instanceof MqttUnsubscribeMessage);
                    processor.processUnSubscribe(ctx.channel(), (MqttUnsubscribeMessage) msg);
                    break;
                case PUBLISH:
                    checkState(msg instanceof MqttPublishMessage);
                    processor.processPublish(ctx.channel(), (MqttPublishMessage) msg);
                    break;
                case PUBREC:
                    processor.processPubRec(ctx.channel(), msg);
                    break;
                case PUBCOMP:
                    processor.processPubComp(ctx.channel(), msg);
                    break;
                case PUBREL:
                    processor.processPubRel(ctx.channel(), msg);
                    break;
                case DISCONNECT:
                    processor.processDisconnect(ctx.channel());
                    break;
                case PUBACK:
                    checkState(msg instanceof MqttPubAckMessage);
                    processor.processPubAck(ctx.channel(), (MqttPubAckMessage) msg);
                    break;
                case PINGREQ:
                    MqttFixedHeader pingHeader = new MqttFixedHeader(
                            MqttMessageType.PINGRESP,
                            false,
                            AT_MOST_ONCE,
                            false,
                            0);
                    MqttMessage pingResp = new MqttMessage(pingHeader);
                    ctx.writeAndFlush(pingResp);
                    break;
                default:
                    log.error("Unkonwn MessageType:{}", messageType);
                    break;
            }
        } catch (Throwable ex) {
            log.error("Exception was caught while processing MQTT message, " + ex.getCause(), ex);
            ctx.fireExceptionCaught(ex);
            ctx.close();
        }
    }

    public void resetProxyHandler() {
        if (proxyHandler != null) {
            proxyHandler.close();
            proxyHandler = null;
        }
    }

    public void close() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection close.");
        }

        if (proxyHandler != null) {
            resetProxyHandler();
        }
        if (cnx != null) {
            cnx.close();
        }
        state = State.Closed;
    }

    public boolean increaseSubscribeTopicsCount(int seq, int count) {
        return topicCountForSequenceId.putIfAbsent(seq, new AtomicInteger(count)) == null;
    }

    public int decreaseSubscribeTopicsCount(int seq) {
        if (topicCountForSequenceId.get(seq) == null) {
            log.warn("Unexpected subscribe behavior for the proxy, respond seq {} " +
                    "but but the seq does not tracked by the proxy. ", seq);
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
