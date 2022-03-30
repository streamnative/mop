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
package io.streamnative.pulsar.handlers.mqtt.support.systemtopic;

import com.google.common.annotations.Beta;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.util.RetryUtil;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * System topic based event service.
 */
@Slf4j
@Beta
public class SystemTopicBasedSystemEventService implements SystemEventService {

    public static final TopicName SYSTEM_EVENT_TOPIC = TopicName.get("pulsar/system/__mqtt_event");
    private final PulsarService pulsarService;
    private final SystemTopicClient<MqttEvent> systemTopicClient;
    private final List<EventListener> listeners;

    private volatile SystemTopicClient.Reader<MqttEvent> reader;
    private final AtomicBoolean initReader = new AtomicBoolean(false);
    private final AtomicInteger maxRetry = new AtomicInteger(0);

    public SystemTopicBasedSystemEventService(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        try {
            this.systemTopicClient = new MQTTEventSystemTopicClient(pulsarService.getClient(), SYSTEM_EVENT_TOPIC);
        } catch (PulsarServerException e) {
            throw new IllegalStateException(e);
        }
        this.listeners = new ArrayList<>();
    }

    @Override
    public void addListener(EventListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public CompletableFuture<Void> sendConnectEvent(ConnectEvent event) {
        checkReader();
        return sendEvent(getMqttEvent(event, ActionType.INSERT))
                .thenRun(() -> {
                    if (log.isDebugEnabled()) {
                        log.debug("send connect event : {}", event);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> sendLWTEvent(LastWillMessageEvent event) {
        checkReader();
        return sendEvent(getMqttEvent(event, ActionType.INSERT))
                .thenRun(() -> {
                    if (log.isDebugEnabled()) {
                        log.debug("send LWT event : {}", event);
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
                    log.error("[{}] send event error.", SYSTEM_EVENT_TOPIC, ex);
                }
                writer.closeAsync();
            });
            return writeFuture.thenAccept(__ -> {});
        }).exceptionally(ex -> {
            log.error("[{}] send event error.", SYSTEM_EVENT_TOPIC, ex);
            return null;
        });
    }

    private MqttEvent getMqttEvent(LastWillMessageEvent event, ActionType actionType) {
        MqttEvent.MqttEventBuilder builder = MqttEvent.builder();
        try {
            String json = JsonUtil.toJson(event);
            log.info("LastWillMessageEven-json : {}", json);
            return builder
                    .key(event.getClientId() + "-LWT")
                    .eventType(EventType.LAST_WILL_MESSAGE)
                    .actionType(actionType)
                    .sourceEvent(json)
                    .build();
        } catch (JsonUtil.ParseJsonException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private MqttEvent getMqttEvent(ConnectEvent event, ActionType actionType) {
        MqttEvent.MqttEventBuilder builder = MqttEvent.builder();
        try {
            return builder
                    .key(event.getClientId())
                    .eventType(EventType.CONNECT)
                    .actionType(actionType)
                    .sourceEvent(JsonUtil.toJson(event))
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
                Throwable cause = FutureUtil.unwrapCompletionException(ex);
                if (cause instanceof PulsarClientException.AlreadyClosedException) {
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
            log.debug("refresh cache for event : {}", msg.getValue());
        }
        MqttEvent value = msg.getValue();
        try {
            switch (value.getEventType()) {
                case CONNECT:
                    ConnectEvent connectEvent = JsonUtil.fromJson((String) value.getSourceEvent(), ConnectEvent.class);
                    value.setSourceEvent(connectEvent);
                    break;
                case LAST_WILL_MESSAGE:
                    LastWillMessageEvent lwtEvent = JsonUtil.fromJson((String) value.getSourceEvent(),
                            LastWillMessageEvent.class);
                    value.setSourceEvent(lwtEvent);
                    break;
                default:
                    break;
            }
            listeners.forEach(listener -> listener.onChange(value));
        } catch (Exception ex) {
            log.error("refresh cache error", ex);
        }
    }
}
