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
package io.streamnative.pulsar.handlers.mqtt.common;

import static io.streamnative.pulsar.handlers.mqtt.common.systemtopic.EventType.CONNECT;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.ConnectEvent;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.EventListener;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.MqttEvent;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy connection manager.
 */
@Slf4j
public class MQTTConnectionManager {

    private final ConcurrentMap<String, Connection> connections;

    @Getter
    private static final HashedWheelTimer sessionExpireInterval =
            new HashedWheelTimer(
                    new DefaultThreadFactory("session-expire-interval"), 1, TimeUnit.SECONDS);

    @Getter
    private final EventListener eventListener;

    private final String advertisedAddress;

    public MQTTConnectionManager(String advertisedAddress) {
        this.advertisedAddress = advertisedAddress;
        this.connections = new ConcurrentHashMap<>(2048);
        this.eventListener = new ConnectEventListener();
    }

    public void addConnection(Connection connection) {
        Connection existing = connections.put(connection.getClientId(), connection);
        if (existing != null) {
            log.warn("The clientId is existed. Close existing connection. CId={}", existing.getClientId());
            existing.disconnect();
        }
    }

    /**
     * create new timeout task to process task by expire interval.
     *
     * @param task   - task
     * @param clientId - client identifier
     * @param interval - expire interval time
     */
    public void newSessionExpireInterval(Consumer<Timeout> task, String clientId, int interval) {
        sessionExpireInterval.newTimeout(timeout -> {
            Connection connection = connections.get(clientId);
            if (connection != null
                    && connection.getState() != Connection.ConnectionState.DISCONNECTED) {
                return;
            }
            task.accept(timeout);
        }, interval, TimeUnit.SECONDS);
    }

    // Must use connections.remove(key, value).
    public void removeConnection(Connection connection) {
        if (connection != null) {
            connections.remove(connection.getClientId(), connection);
        }
    }

    public Connection getConnection(String clientId) {
        return connections.get(clientId);
    }

    public void close() {
        connections.values().forEach(connection -> connection.getChannel().close());
    }

    class ConnectEventListener implements EventListener {

        @Override
        public void onChange(MqttEvent event) {
            if (event.getEventType() == CONNECT) {
                ConnectEvent connectEvent = (ConnectEvent) event.getSourceEvent();
                if (!connectEvent.getAddress().equals(advertisedAddress)) {
                    Connection connection = getConnection(connectEvent.getClientId());
                    if (connection != null) {
                        log.warn("[ConnectEvent] close existing connection : {}", connection);
                        connection.disconnect();
                    }
                }
            }
        }
    }
}
