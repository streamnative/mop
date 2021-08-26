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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Value object to maintain the information of single connection, like ClientID, Channel, and clean
 * session flag.
 */
public class ConnectionDescriptor {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionDescriptor.class);

    /**
     * Connection state.
     */
    public enum ConnectionState {
        // Connection states
        DISCONNECTED,
        SENDACK,
        SESSION_CREATED,
        MESSAGES_REPUBLISHED,
        ESTABLISHED,
        // Disconnection states
        SUBSCRIPTIONS_REMOVED,
        MESSAGES_DROPPED,
        INTERCEPTORS_NOTIFIED;
    }

    public final String clientID;
    private final Channel channel;
    public final boolean cleanSession;
    private final AtomicReference<ConnectionState> channelState = new AtomicReference<>(ConnectionState.DISCONNECTED);

    public ConnectionDescriptor(String clientID, Channel session, boolean cleanSession) {
        this.clientID = clientID;
        this.channel = session;
        this.cleanSession = cleanSession;
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
        LOG.info("Closing connection descriptor. MqttClientId = {}.", clientID);
        final boolean success = assignState(ConnectionState.SUBSCRIPTIONS_REMOVED, ConnectionState.DISCONNECTED);
        if (!success) {
            return false;
        }
        this.channel.close();
        return true;
    }

    public String getUsername() {
        return NettyUtils.userName(this.channel);
    }

    public void abort() {
        LOG.info("Closing connection descriptor. MqttClientId = {}.", clientID);
        this.channel.close();
    }

    public boolean assignState(ConnectionState expected, ConnectionState newState) {
        LOG.debug(
                "Updating state of connection descriptor. MqttClientId = {}, expectedState = {}, newState = {}.",
                clientID,
                expected,
                newState);
        boolean retval = channelState.compareAndSet(expected, newState);
        if (!retval) {
            LOG.error(
                    "Unable to update state of connection descriptor."
                            + " MqttclientId = {}, expectedState = {}, newState = {}.",
                    clientID,
                    expected,
                    newState);
        }
        return retval;
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
