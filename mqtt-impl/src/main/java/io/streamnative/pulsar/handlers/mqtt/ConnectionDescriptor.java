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

import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.ESTABLISHED;
import static io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptor.ConnectionState.SEND_ACK;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
public class ConnectionDescriptor {

    /**
     * Connection state.
     */
    public enum ConnectionState {
        DISCONNECTED,
        SEND_ACK,
        ESTABLISHED,
    }

    public final String clientID;
    @Getter
    private final Channel channel;
    public final boolean cleanSession;
    private final AtomicReference<ConnectionState> channelState = new AtomicReference<>(ConnectionState.DISCONNECTED);

    public ConnectionDescriptor(String clientID, Channel session, boolean cleanSession) {
        this.clientID = clientID;
        this.channel = session;
        this.cleanSession = cleanSession;
    }

    public void sendConnAck() {
        boolean ret = assignState(DISCONNECTED, SEND_ACK);
        if (ret) {
            MqttConnAckMessage ackMessage = MqttMessageUtils.connAck(MqttConnectReturnCode.CONNECTION_ACCEPTED);
            channel.writeAndFlush(ackMessage).addListener(future -> {
                if (future.isSuccess()) {
                    if (log.isDebugEnabled()) {
                        log.debug("The CONNECT message has been processed. CId={}", clientID);
                    }
                    assignState(SEND_ACK, ESTABLISHED);
                }
            });
        } else {
            channel.close();
        }
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

    public void removeSubscriptions() {
        removeConsumers();
        if (cleanSession) {
            Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = NettyUtils
                    .getTopicSubscriptions(channel);
            // For producer doesn't bind subscriptions
            if (topicSubscriptions != null) {
                topicSubscriptions.forEach((k, v) -> {
                    k.unsubscribe(NettyUtils.getClientId(channel));
                    v.getLeft().delete();
                });
            }
        }
    }

    public ChannelPromise writeAndFlush(Object payload) {
        ChannelPromise promise = channel.newPromise();
        this.channel.writeAndFlush(payload, promise);
        return promise;
    }

    public boolean doesNotUseChannel(Channel channel) {
        return !(this.channel.equals(channel));
    }

    public boolean close() {
        if (log.isInfoEnabled()) {
            log.info("Closing connection descriptor. MqttClientId = {}.", clientID);
        }
        final boolean success = assignState(ConnectionState.ESTABLISHED, ConnectionState.DISCONNECTED);
        if (!success) {
            return false;
        }
        this.channel.close();
        return true;
    }

    public void abort() {
        if (log.isInfoEnabled()) {
            log.info("Closing connection descriptor. MqttClientId = {}.", clientID);
        }
        this.channel.close();
    }

    private boolean assignState(ConnectionState expected, ConnectionState newState) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Updating state of connection descriptor. CId = {}, currentState = {}, "
                            + "expectedState = {}, newState = {}.",
                    clientID,
                    channelState.get(),
                    expected,
                    newState);
        }
        boolean ret = channelState.compareAndSet(expected, newState);
        if (!ret) {
            log.error(
                    "Unable to update state of connection descriptor."
                            + " CId = {}, currentState = {}, expectedState = {}, newState = {}.",
                    clientID,
                    channelState.get(),
                    expected,
                    newState);
        }
        return ret;
    }

    @Override
    public String toString() {
        return "ConnectionDescriptor{" + "clientID=" + clientID + ", cleanSession=" + cleanSession + ", state="
                + channelState.get() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionDescriptor that = (ConnectionDescriptor) o;

        if (clientID != null ? !clientID.equals(that.clientID) : that.clientID != null) {
            return false;
        }
        return !(channel != null ? !channel.equals(that.channel) : that.channel != null);
    }

    @Override
    public int hashCode() {
        int result = clientID != null ? clientID.hashCode() : 0;
        result = 31 * result + (channel != null ? channel.hashCode() : 0);
        return result;
    }
}
