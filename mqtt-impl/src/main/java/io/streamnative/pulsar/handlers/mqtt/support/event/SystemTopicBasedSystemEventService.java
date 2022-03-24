/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.support.event;

import com.google.common.annotations.Beta;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.util.RetryUtil;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/**
 * System topic based event service.
 */
@Slf4j
@Beta
public class SystemTopicBasedSystemEventService implements SystemEventService {

    private final PulsarService pulsarService;
    private final TopicName topicName;
    private final SystemTopicClient<MqttEvent> systemTopicClient;
    private final Map<String, SourceEvent> eventCache;
    private final ConcurrentMap<String, String> clusterClientIds;
    private final List<EventListener> listeners;

    private volatile SystemTopicClient.Reader<MqttEvent> reader;
    private final AtomicBoolean initReader = new AtomicBoolean(false);
    private final AtomicInteger maxRetry = new AtomicInteger(0);

    public SystemTopicBasedSystemEventService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        NamespaceName heartbeatNamespace = NamespaceService.getHeartbeatNamespaceV2(
                pulsarService.getAdvertisedAddress(), pulsarService.getConfig());
        this.topicName = TopicName.get("pulsar/system/__mqtt_event");
        try {
            this.systemTopicClient = new MQTTEventSystemTopicClient(pulsarService.getClient(), topicName);
        } catch (PulsarServerException e) {
            throw new RuntimeException(e);
        }
        this.eventCache = new ConcurrentHashMap<>();
        this.clusterClientIds = new ConcurrentHashMap<>(2048);
        this.listeners = new ArrayList<>();
        this.addListener(new ConnectEventListener());
    }

    public void addListener(EventListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public boolean containsClientId(String clientId) {
        checkReader();
        return clusterClientIds.containsKey(clientId);
    }

    @Override
    public CompletableFuture<Void> sendConnectEvent(Connection connection) {
        ConnectEvent connectEvent = ConnectEvent.builder()
                .clientId(connection.getClientId())
                .ip(NettyUtils.getIp(connection.getChannel()))
                .build();
        return sendEvent(getMqttEvent(connectEvent, ActionType.INSERT))
                .thenRun(() -> {
                    if (log.isDebugEnabled()) {
                        log.debug("send connect event : {}", connectEvent);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> sendDisconnectEvent(Connection connection) {
        if (!connection.getChannel().isActive()) {
            return CompletableFuture.completedFuture(null);
        }
        ConnectEvent disconnectEvent = ConnectEvent.builder()
                .clientId(connection.getClientId())
                .ip(NettyUtils.getIp(connection.getChannel()))
                .build();
        return sendEvent(getMqttEvent(disconnectEvent, ActionType.DELETE))
                .thenRun(() -> {
                    if (log.isDebugEnabled()) {
                        log.debug("send disconnect event : {}", disconnectEvent);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> sendEvent(MqttEvent event) {
        CompletableFuture<SystemTopicClient.Writer<MqttEvent>> writerFuture = systemTopicClient.newWriterAsync();
        return writerFuture.thenCompose(writer -> {
            CompletableFuture<MessageId> writeFuture = ActionType.DELETE.equals(event.getActionType())
                    ? writer.deleteAsync(event) : writer.writeAsync(event);
            writeFuture.whenComplete((__, ex) -> {
                if (ex != null) {
                    log.error("[{}] send event error.", topicName, ex);
                }
                writer.closeAsync();
            });
            return writeFuture.thenAccept(__ -> {});
        }).exceptionally(ex -> {
            log.error("[{}] send event error.", topicName, ex);
            return null;
        });
    }

    private MqttEvent getMqttEvent(ConnectEvent connectEvent, ActionType actionType) {
        MqttEvent.MqttEventBuilder builder = MqttEvent.builder();
        try {
            return builder
                    .key(connectEvent.getClientId())
                    .eventType(EventType.CONNECT)
                    .actionType(actionType)
                    .sourceEvent(JsonUtil.toJson(connectEvent))
                    .build();
        } catch (JsonUtil.ParseJsonException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected CompletableFuture<SystemTopicClient.Reader<MqttEvent>> createReader() {
        CompletableFuture<SystemTopicClient.Reader<MqttEvent>> result = new CompletableFuture<>();
        Backoff backoff = new Backoff(1, TimeUnit.SECONDS,
                3, TimeUnit.SECONDS,
                10, TimeUnit.SECONDS);
        RetryUtil.retryAsynchronously(systemTopicClient::newReaderAsync, backoff, pulsarService.getExecutor(), result);
        return result;
    }

    @Override
    public void start() {
        CompletableFuture<Boolean> checkNamespaceFuture = pulsarService
                .getPulsarResources()
                .getNamespaceResources()
                .namespaceExistsAsync(NamespaceName.SYSTEM_NAMESPACE);
        checkNamespaceFuture.thenAccept(ret -> {
            if (ret) {
                startReader();
            } else {
                if (maxRetry.incrementAndGet() < 10) {
                    pulsarService.getExecutor().schedule(this::start, 1, TimeUnit.SECONDS);
                }
            }
        }).exceptionally(ex -> {
            log.error("check system namespace : {} error", NamespaceName.SYSTEM_NAMESPACE, ex);
            return null;
        });
    }

    @Override
    public void close() {
        closeReader();
        eventCache.clear();
    }

    private void startReader() {
        if (initReader.compareAndSet(false, true)) {
            createReader().thenAccept(reader -> {
                this.reader = reader;
                readEvent();
            }).exceptionally(ex -> {
                initReader.set(false);
                log.error("create reader error", ex);
                return null;
            });
        }
    }

    private void checkReader() {
        if (!initReader.get()) {
            startReader();
        }
    }

    private void closeReader() {
        if (initReader.compareAndSet(true, false)) {
            reader.closeAsync();
        }
    }

    private void readEvent() {
        reader.readNextAsync().whenComplete((msg, ex) -> {
            if (ex == null) {
                refreshCache(msg);
                readEvent();
            } else {
                if (ex instanceof PulsarClientException.AlreadyClosedException) {
                    log.error("Read more topic policies exception, close the read now!", ex);
                    closeReader();
                } else {
                    log.warn("Read more topic polices exception, read again.", ex);
                    readEvent();
                }
            }
        });
    }

    private void refreshCache(Message<MqttEvent> msg) {
        if (log.isDebugEnabled()) {
            log.info("refresh cache for event : {}", msg.getValue());
        }
        MqttEvent value = msg.getValue();
        try {
            switch (value.getEventType()) {
                case CONNECT:
                    ConnectEvent connectEvent = JsonUtil.fromJson((String) value.getSourceEvent(), ConnectEvent.class);
                    switch (value.getActionType()) {
                        case INSERT:
                            eventCache.putIfAbsent(msg.getKey(), connectEvent);
                            break;
                        case DELETE:
                            eventCache.remove(msg.getKey());
                            break;
                        case UPDATE:
                        case NONE:
                        default:
                            log.warn("Nothing to do with event : {}", msg.getValue());
                            break;
                    }
                value.setSourceEvent(connectEvent);
                break;
            }
            listeners.forEach(listener -> listener.onChange(value));
        } catch (Exception ex) {
            log.error("refresh cache error", ex);
        }
    }

    class ConnectEventListener implements EventListener {

        @Override
        public void onChange(MqttEvent event) {
            ConnectEvent connectEvent = (ConnectEvent) event.getSourceEvent();
            ActionType actionType = event.getActionType();
            switch (actionType) {
                case INSERT:
                    clusterClientIds.put(connectEvent.getClientId(), connectEvent.getIp());
                    break;
                case DELETE:
                    clusterClientIds.remove(connectEvent.getClientId());
                    break;
                default:
                    log.warn("do nothing with connect event : {} type : {}", connectEvent, actionType);
            }
            log.info("clusterClientIds : {}", clusterClientIds);
        }
    }
}
