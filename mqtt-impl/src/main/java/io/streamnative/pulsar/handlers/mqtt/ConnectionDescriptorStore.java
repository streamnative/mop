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

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.extern.slf4j.Slf4j;

/**
 * Connection description store for maintaining MQTT connections.
 */
@Slf4j
public class ConnectionDescriptorStore implements IConnectionsManager {

    private final ConcurrentMap<String, ConnectionDescriptor> connectionDescriptors = new ConcurrentHashMap<>();

    private static final ConnectionDescriptorStore instance = new ConnectionDescriptorStore();
    private ConnectionDescriptorStore(){

    }

    public static ConnectionDescriptorStore getInstance(){
        return instance;
    }

    public ChannelPromise sendMessage(MqttMessage message, Integer messageID, String clientID) {
        final MqttMessageType messageType = message.fixedHeader().messageType();
        try {
            if (log.isDebugEnabled()) {
                if (messageID != null) {
                    log.debug("Sending {} message CId=<{}>, messageId={}", messageType, clientID, messageID);
                } else {
                    log.debug("Sending {} message CId=<{}>", messageType, clientID);
                }
            }

            ConnectionDescriptor descriptor = connectionDescriptors.get(clientID);
            if (descriptor == null) {
                if (messageID != null) {
                    log.error("Client has just disconnected. {} message could not be sent. CId=<{}>, messageId={}",
                        messageType, clientID, messageID);
                } else {
                    log.error("Client has just disconnected. {} could not be sent. CId=<{}>", messageType, clientID);
                }
                /*
                 * If the client has just disconnected, its connection descriptor will be null. We
                 * don't have to make the broker crash: we'll just discard the PUBACK message.
                 */
            } else {
                return descriptor.writeAndFlush(message);
            }
        } catch (Throwable e) {
            String errorMsg = "Unable to send " + messageType + " message. CId=<" + clientID + ">";
            if (messageID != null) {
                errorMsg += ", messageId=" + messageID;
            }
            log.error(errorMsg, e);
        }
        return null;
    }

    public ConnectionDescriptor addConnection(ConnectionDescriptor descriptor) {
        return connectionDescriptors.putIfAbsent(descriptor.clientID, descriptor);
    }

    public boolean removeConnection(ConnectionDescriptor descriptor) {
        return connectionDescriptors.remove(descriptor.clientID, descriptor);
    }

    public ConnectionDescriptor getConnection(String clientID) {
        return connectionDescriptors.get(clientID);
    }

    @Override
    public boolean isConnected(String clientID) {
        return connectionDescriptors.containsKey(clientID);
    }

    @Override
    public int getActiveConnectionsNo() {
        return connectionDescriptors.size();
    }

    @Override
    public Collection<String> getConnectedClientIds() {
        return connectionDescriptors.keySet();
    }

    @Override
    public boolean closeConnection(String clientID, boolean closeImmediately) {
        ConnectionDescriptor descriptor = connectionDescriptors.get(clientID);
        if (descriptor == null) {
            log.error("Connection descriptor doesn't exist. MQTT connection cannot be closed. CId=<{}>, "
                    + "closeImmediately={}", clientID, closeImmediately);
            return false;
        }
        if (closeImmediately) {
            descriptor.abort();
            return true;
        } else {
            return descriptor.close();
        }
    }

}
