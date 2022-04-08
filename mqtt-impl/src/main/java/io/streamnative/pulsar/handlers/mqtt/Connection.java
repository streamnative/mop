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
package io.streamnative.pulsar.handlers.mqtt;

import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.ESTABLISHED;
import static io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils.ATTR_KEY_CONNECTION;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import io.netty.channel.Channel;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.exception.restrictions.InvalidSessionExpireIntervalException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ServerRestrictions;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarEventListener;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandlerFactory;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.WillMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Value object to maintain the information of single connection, like ClientID, Channel, and clean
 * session flag.
 */
@Slf4j
public class Connection {

    @Getter
    private final String clientId;
    @Getter
    private final Channel channel;
    @Getter
    private final int protocolVersion;
    @Getter
    private final WillMessage willMessage;
    @Getter
    private final String userRole;
    @Getter
    private final MQTTConnectionManager manager;
    @Getter
    private final TopicSubscriptionManager topicSubscriptionManager;
    @Getter
    private final MqttConnectMessage connectMessage;
    @Getter
    private final AckHandler ackHandler;
    @Getter
    private final ClientRestrictions clientRestrictions;
    @Getter
    private final ServerRestrictions serverRestrictions;
    volatile ConnectionState connectionState = DISCONNECTED;
    @Getter
    private volatile int serverCurrentReceiveCounter = 0;
    private final PulsarEventCenter eventCenter;
    private final List<PulsarEventListener> listeners;

    private static final AtomicReferenceFieldUpdater<Connection, ConnectionState> channelState =
            newUpdater(Connection.class, ConnectionState.class, "connectionState");

    private static final AtomicIntegerFieldUpdater<Connection> SERVER_CURRENT_RECEIVE_PUB_MAXIMUM_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Connection.class, "serverCurrentReceiveCounter");

    static ChannelException channelInactiveException = new ChannelException("Channel is inactive");

    Connection(ConnectionBuilder builder) {
        this.clientId = builder.clientId;
        this.protocolVersion = builder.protocolVersion;
        this.willMessage = builder.willMessage;
        this.clientRestrictions = builder.clientRestrictions;
        this.serverRestrictions = builder.serverRestrictions;
        this.userRole = builder.userRole;
        this.channel = builder.channel;
        this.manager = builder.connectionManager;
        this.connectMessage = builder.connectMessage;
        this.ackHandler = AckHandlerFactory.of(protocolVersion).getAckHandler();
        this.channel.attr(ATTR_KEY_CONNECTION).set(this);
        this.topicSubscriptionManager = new TopicSubscriptionManager();
        this.addIdleStateHandler();
        this.eventCenter = builder.eventCenter;
        this.listeners = Collections.synchronizedList(new ArrayList<>());
        this.manager.addConnection(this);
    }

    private void addIdleStateHandler() {
        ChannelPipeline pipeline = channel.pipeline();
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0,
                Math.round(clientRestrictions.getKeepAliveTime() * 1.5f)));
    }

    public ChannelFuture sendConnAck() {
        return ackHandler.sendConnAck(this);
    }

    public ChannelFuture send(MqttMessage mqttMessage) {
        if (!channel.isActive()) {
            log.error("send mqttMessage : {} failed due to channel is inactive.", mqttMessage);
            return channel.newFailedFuture(channelInactiveException);
        }
        return channel.writeAndFlush(mqttMessage).addListener(future -> {
            if (!future.isSuccess()) {
                log.error("send mqttMessage : {} failed", mqttMessage, future.cause());
            }
        });
    }

    public ChannelFuture sendThenClose(MqttMessage mqttMessage) {
        if (!channel.isActive()) {
            log.error("send mqttMessage : {} failed due to channel is inactive.", mqttMessage);
            return channel.newFailedFuture(channelInactiveException);
        }
        channel.writeAndFlush(mqttMessage).addListener(future -> {
            if (!future.isSuccess()) {
                log.error("send mqttMessage : {} failed", mqttMessage, future.cause());
            }
        });
        return channel.close();
    }

    public CompletableFuture<Void> close() {
        return close(false)
                .exceptionally(ex -> {
                    log.error("close connection : {} error", this, ex);
                    return null;
                });
    }

    public CompletableFuture<Void> close(boolean force) {
        log.info("Closing connection clientId = {} force : {}", clientId, force);
        if (force) {
            assignState(ESTABLISHED, DISCONNECTED);
            if (MqttUtils.isMqtt5(protocolVersion)) {
                MqttMessage mqttMessage = MqttMessageBuilders
                        .disconnect()
                        .reasonCode(Mqtt5DisConnReasonCode.SESSION_TAKEN_OVER.byteValue())
                        .build();
                sendThenClose(mqttMessage);
            } else {
                channel.close();
            }
        }
        // unregister all listener
        for (PulsarEventListener listener : listeners) {
            eventCenter.unRegister(listener);
        }
        if (clientRestrictions.isCleanSession()) {
            return topicSubscriptionManager.removeSubscriptions();
        }
        // when use mqtt5.0 we need to use session expire interval to remove session.
        // but mqtt protocol version lower than 5.0 we don't do that.
        if (MqttUtils.isMqtt5(protocolVersion) && !clientRestrictions.isSessionNeverExpire()) {
            if (clientRestrictions.isSessionExpireImmediately()) {
                return topicSubscriptionManager.removeSubscriptions();
            } else {
                manager.newSessionExpireInterval(__ -> topicSubscriptionManager.removeSubscriptions(),
                        clientId, clientRestrictions.getSessionExpireInterval());
                return CompletableFuture.completedFuture(null);
            }
        } else {
            // remove subscription consumer if we don't need to clean session.
            topicSubscriptionManager.removeSubscriptionConsumers();
            return CompletableFuture.completedFuture(null);
        }
    }

    public boolean assignState(ConnectionState expected, ConnectionState newState) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Updating state of connection = {}, currentState = {}, "
                            + "expectedState = {}, newState = {}.",
                    this,
                    channelState.get(this),
                    expected,
                    newState);
        }
        boolean ret = channelState.compareAndSet(this, expected, newState);
        if (!ret) {
            log.error(
                    "Unable to update state of connection = {}, currentState = {}, expectedState = {}, newState = {}.",
                    this,
                    channelState.get(this),
                    expected,
                    newState);
        }
        return ret;
    }

    public void addTopicChangeListener(PulsarEventListener eventListener) {
        listeners.add(eventListener);
        eventCenter.register(eventListener);
    }

    public ConnectionState getState() {
        return channelState.get(this);
    }

    public void updateSessionExpireInterval(int newSessionInterval) throws InvalidSessionExpireIntervalException {
       clientRestrictions.updateExpireInterval(newSessionInterval);
    }

    @Override
    public String toString() {
        return "Connection{" + "clientId=" + clientId + ", channel=" + channel
                + ", cleanSession=" + clientRestrictions.isCleanSession() + ", state="
                + channelState.get(this) + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Connection that = (Connection) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, channel);
    }

    public void incrementServerReceivePubMessage() {
        SERVER_CURRENT_RECEIVE_PUB_MAXIMUM_UPDATER.addAndGet(this, 1);
    }

    public void decrementServerReceivePubMessage() {
        SERVER_CURRENT_RECEIVE_PUB_MAXIMUM_UPDATER.decrementAndGet(this);
    }

    public int getServerReceivePubMessage() {
        return SERVER_CURRENT_RECEIVE_PUB_MAXIMUM_UPDATER.get(this);
    }

    /**
     * Connection state.
     */
    public enum ConnectionState {
        DISCONNECTED,
        CONNECT_ACK,
        ESTABLISHED,
    }

    public static ConnectionBuilder builder(){
        return new ConnectionBuilder();
    }

    public static class ConnectionBuilder {
        private int protocolVersion;
        private String clientId;
        private String userRole;
        private WillMessage willMessage;
        private Channel channel;
        private MQTTConnectionManager connectionManager;
        private MqttConnectMessage connectMessage;
        private ClientRestrictions clientRestrictions;
        private ServerRestrictions serverRestrictions;
        private PulsarEventCenter eventCenter;

        public ConnectionBuilder protocolVersion(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            return this;
        }

        public ConnectionBuilder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public ConnectionBuilder userRole(String userRole) {
            this.userRole = userRole;
            return this;
        }

        public ConnectionBuilder willMessage(WillMessage willMessage) {
            this.willMessage = willMessage;
            return this;
        }

        public ConnectionBuilder clientRestrictions(ClientRestrictions clientRestrictions){
            this.clientRestrictions = clientRestrictions;
            return this;
        }

        public ConnectionBuilder serverRestrictions(ServerRestrictions serverRestrictions){
            this.serverRestrictions = serverRestrictions;
            return this;
        }

        public ConnectionBuilder channel(Channel channel) {
            this.channel = channel;
            return this;
        }

        public ConnectionBuilder connectionManager(MQTTConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public ConnectionBuilder connectMessage(MqttConnectMessage connectMessage) {
            this.connectMessage = connectMessage;
            return this;
        }

        public ConnectionBuilder eventCenter(PulsarEventCenter eventCenter) {
            this.eventCenter = eventCenter;
            return this;
        }

        public Connection build() {
            return new Connection(this);
        }
    }
}
