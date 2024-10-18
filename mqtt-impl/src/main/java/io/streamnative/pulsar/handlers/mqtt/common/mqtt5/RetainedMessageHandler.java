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
package io.streamnative.pulsar.handlers.mqtt.common.mqtt5;

import static io.streamnative.pulsar.handlers.mqtt.common.systemtopic.EventType.RETAINED_MESSAGE;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.common.TopicFilterImpl;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.EventListener;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.MqttEvent;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.RetainedMessageEvent;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.SystemEventService;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.SystemTopicBasedSystemEventService;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.common.utils.RetainedMessage;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetainedMessageHandler {

    @Getter
    private final EventListener eventListener;
    private final SystemEventService eventService;
    private final Map<String, RetainedMessage> retainedMessages = new ConcurrentHashMap<>();

    private final boolean isSystemTopicEnabled;

    public RetainedMessageHandler(SystemEventService eventService) {
        this.eventService = eventService;
        this.isSystemTopicEnabled = eventService instanceof SystemTopicBasedSystemEventService;
        this.eventListener = new RetainedMessageEventListener();
    }

    public CompletableFuture<Void> addRetainedMessage(MqttPublishMessage retainedMessage) {
        if (isSystemTopicEnabled) {
            RetainedMessageEvent event = RetainedMessageEvent
                    .builder()
                    .retainedMessage(MqttMessageUtils.createRetainedMessage(retainedMessage))
                    .build();
            eventService.sendRetainedEvent(event);
        } else {
            // Standalone mode
            addRetainedMessage(MqttMessageUtils.createRetainedMessage(retainedMessage));
        }
        return CompletableFuture.completedFuture(null);
    }

    public Optional<String> getRetainedTopic(String topic) {
        if (MqttUtils.isRegexFilter(topic)) {
            TopicFilterImpl topicFilter = new TopicFilterImpl(topic);
            return retainedMessages.keySet().stream().filter(topicFilter::test).findFirst();
        }
        return retainedMessages.keySet().stream().filter(key -> key.equals(topic)).findFirst();
    }

    public RetainedMessage getRetainedMessage(String topic) {
        return retainedMessages.get(topic);
    }

    private void addRetainedMessage(RetainedMessage msg) {
        String topicName = msg.getTopic();
        if (msg.getPayload().length == 0) {
            retainedMessages.remove(topicName);
        } else {
            retainedMessages.put(topicName, msg);
        }
    }

    class RetainedMessageEventListener implements EventListener {

        @Override
        public void onChange(MqttEvent event) {
            if (event.getEventType() == RETAINED_MESSAGE) {
                RetainedMessageEvent retainedEvent = (RetainedMessageEvent) event.getSourceEvent();
                if (log.isDebugEnabled()) {
                    log.debug("add retained message : {}", retainedEvent.getRetainedMessage());
                }
                addRetainedMessage(retainedEvent.getRetainedMessage());
            }
        }
    }
}
