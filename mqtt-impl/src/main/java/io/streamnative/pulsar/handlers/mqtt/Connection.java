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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.SessionExpireInterval;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandlerDelegate;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.WillMessage;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;

/**
 * Value object to maintain the information of single connection, like ClientID, Channel, and clean
 * session flag.
 */
@Slf4j
public class Connection {

    @Getter
    private String clientId;
    @Getter
    private final Channel channel;
    @Getter
    private final boolean cleanSession;
    @Getter
    private final int protocolVersion;
    @Getter
    private final WillMessage willMessage;
    @Getter
    private volatile int sessionExpireInterval;
    volatile ConnectionState connectionState = DISCONNECTED;
    @Getter
    private int clientReceiveMaximum; // mqtt 5.0 specification.
    @Getter
    private int serverReceivePubMaximum;
    @Getter
    private volatile int serverCurrentReceiveCounter = 0;
    @Getter
    private String userRole;
    @Getter
    private final MQTTConnectionManager manager;
    @Getter
    private final MqttConnectMessage connectMessage;
    private final AckHandlerDelegate delegate;
    @Getter
    private final int keepAliveTime;

    private static final AtomicReferenceFieldUpdater<Connection, ConnectionState> channelState =
            newUpdater(Connection.class, ConnectionState.class, "connectionState");

    private static final AtomicIntegerFieldUpdater<Connection> SESSION_EXPIRE_INTERVAL_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Connection.class, "sessionExpireInterval");

    private static final AtomicIntegerFieldUpdater<Connection> SERVER_CURRENT_RECEIVE_PUB_MAXIMUM_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Connection.class, "serverCurrentReceiveCounter");

    Connection(ConnectionBuilder builder) {
        this.clientId = builder.clientId;
        this.protocolVersion = builder.protocolVersion;
        this.cleanSession = builder.cleanSession;
        this.willMessage = builder.willMessage;
        this.sessionExpireInterval = builder.sessionExpireInterval;
        this.clientReceiveMaximum = builder.clientReceiveMaximum;
        this.serverReceivePubMaximum = builder.serverReceivePubMaximum;
        this.userRole = builder.userRole;
        this.channel = builder.channel;
        this.manager = builder.connectionManager;
        this.manager.addConnection(this);
        this.connectMessage = builder.connectMessage;
        this.keepAliveTime = builder.keepAliveTime;
        this.delegate = AckHandlerDelegate.delegate(this, builder.ackHandler);
        this.channel.attr(ATTR_KEY_CONNECTION).set(this);
        this.addIdleStateHandler();
    }

    private void addIdleStateHandler() {
        ChannelPipeline pipeline = channel.pipeline();
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0,
                Math.round(keepAliveTime * 1.5f)));
    }

    public AckHandlerDelegate ackHandler() {
        return this.delegate;
    }

    public ChannelFuture send(MqttMessage mqttMessage) {
        return channel.writeAndFlush(mqttMessage).addListener(future -> {
            if (!future.isSuccess()) {
                future.cause().printStackTrace();
                log.error("send mqttMessage : {} failed", mqttMessage, future.cause());
            }
        });
    }

    public void sendThenClose(MqttMessage mqttMessage) {
        channel.writeAndFlush(mqttMessage).addListener(future -> {
            if (!future.isSuccess()) {
                log.error("send mqttMessage : {} failed", mqttMessage, future.cause());
            }
        });
        channel.close();
    }

    public void removeConsumers() {
        Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = NettyUtils
                .getTopicSubscriptions(channel);
        // For producer doesn't bind subscriptions
        if (topicSubscriptions != null) {
            topicSubscriptions.forEach((k, v) -> {
                try {
                    v.getLeft().removeConsumer(v.getRight());
                } catch (BrokerServiceException ex) {
                    log.warn("subscription [{}] remove consumer {} error", v.getLeft(), v.getRight(), ex);
                }
            });
        }
    }

    private void doRemoveSubscriptions() {
        Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = NettyUtils
                .getTopicSubscriptions(channel);
        // For producer doesn't bind subscriptions
        if (topicSubscriptions != null) {
            topicSubscriptions.forEach((k, v) -> {
                k.unsubscribe(this.clientId);
                v.getLeft().delete();
            });
        }
    }

    public void removeSubscriptions() {
        removeConsumers();
        if (cleanSession) {
            doRemoveSubscriptions();
            // when mqtt client support session expire interval variable
        } else {
            // when use mqtt5.0 we need to use session expire interval to remove session.
            // but mqtt protocol version lower than 5.0 we don't do that.
            if (MqttUtils.isMqtt5(protocolVersion)
                    && SESSION_EXPIRE_INTERVAL_UPDATER.get(this)
                    != SessionExpireInterval.NEVER_EXPIRE.getSecondTime()) {
                if (sessionExpireInterval == SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime()) {
                    doRemoveSubscriptions();
                } else {
                    manager.newSessionExpireInterval(__ ->
                            doRemoveSubscriptions(), clientId, SESSION_EXPIRE_INTERVAL_UPDATER.get(this));
                }
            }
        }
    }

    public void close() {
        close(false);
    }

    public void close(boolean force) {
        if (log.isInfoEnabled()) {
            log.info("Closing connection. clientId = {}.", clientId);
        }
        if (!force) {
            assignState(ESTABLISHED, DISCONNECTED);
        }
        // Support mqtt 5
        if (MqttUtils.isMqtt5(protocolVersion)) {
            MqttMessage mqttDisconnectionAckMessage =
                    MqttDisConnAckMessageHelper.createMqtt5(Mqtt5DisConnReasonCode.NORMAL);
            channel.writeAndFlush(mqttDisconnectionAckMessage);
        }
        this.channel.close();
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

    public ConnectionState getConnectionState(Connection connection) {
        return channelState.get(connection);
    }

    public void updateSessionExpireInterval(int newSessionInterval) {
        SESSION_EXPIRE_INTERVAL_UPDATER.set(this, newSessionInterval);
    }

    @Override
    public String toString() {
        return "Connection{" + "clientId=" + clientId + ", channel=" + channel
                + ", cleanSession=" + cleanSession + ", state="
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

    /**
     * Check the session expire interval is valid.
     *
     * @param sessionExpireInterval session expire interval second time
     * @return whether param is valid.
     */
    public boolean checkIsLegalExpireInterval(Integer sessionExpireInterval) {
        int sei = SESSION_EXPIRE_INTERVAL_UPDATER.get(this);
        int zeroSecond = SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime();
        return sei > zeroSecond && sessionExpireInterval >= zeroSecond;
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
        private boolean cleanSession;
        private WillMessage willMessage;
        private int clientReceiveMaximum;
        private int serverReceivePubMaximum;
        private int sessionExpireInterval;
        private Channel channel;
        private MQTTConnectionManager connectionManager;
        private MqttConnectMessage connectMessage;
        private int keepAliveTime;
        private AckHandler ackHandler;

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

        public ConnectionBuilder cleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        public ConnectionBuilder clientReceiveMaximum(int clientReceiveMaximum) {
            this.clientReceiveMaximum = clientReceiveMaximum;
            return this;
        }

        public ConnectionBuilder ackHandler(AckHandler handler){
            this.ackHandler = handler;
            return this;
        }

        public ConnectionBuilder serverReceivePubMaximum(int serverReceivePubMaximum) {
            this.serverReceivePubMaximum = serverReceivePubMaximum;
            return this;
        }

        public ConnectionBuilder sessionExpireInterval(int sessionExpireInterval) {
            this.sessionExpireInterval = sessionExpireInterval;
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

        public ConnectionBuilder keepAliveTime(int keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        public Connection build() {
            return new Connection(this);
        }
    }
}
