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
package io.streamnative.pulsar.handlers.mqtt.support;

import static io.streamnative.pulsar.handlers.mqtt.support.systemtopic.EventType.LAST_WILL_MESSAGE;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.createMqttWillMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.MQTTSubscriptionManager;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.EventListener;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.LastWillMessageEvent;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.MqttEvent;
import io.streamnative.pulsar.handlers.mqtt.utils.WillMessage;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;

@Slf4j
public class WillMessageHandler {

    private final PulsarService pulsarService;
    private final MQTTSubscriptionManager mqttSubscriptionManager;
    private final MQTTConnectionManager connectionManager;
    private final String advertisedAddress;
    @Getter
    private final EventListener eventListener;
    private final MQTTService mqttService;

    public WillMessageHandler(MQTTService mqttService) {
        this.mqttService = mqttService;
        this.pulsarService = mqttService.getPulsarService();
        this.mqttSubscriptionManager = mqttService.getSubscriptionManager();
        this.connectionManager = mqttService.getConnectionManager();
        this.advertisedAddress = mqttService.getPulsarService().getAdvertisedAddress();
        this.eventListener = new LastWillMessageEventListener();
    }

    public void fireWillMessage(String clientId, WillMessage willMessage) {
        if (StringUtils.isNotBlank(clientId) && mqttService.isSystemTopicEnabled()) {
            LastWillMessageEvent lwt = LastWillMessageEvent
                    .builder()
                    .clientId(clientId)
                    .willMessage(willMessage)
                    .address(pulsarService.getAdvertisedAddress())
                    .build();
            mqttService.getEventService().sendLWTEvent(lwt);
        }
        List<Pair<String, String>> subscriptions = mqttSubscriptionManager.findMatchTopic(willMessage.getTopic());
        MqttPublishMessage msg = createMqttWillMessage(willMessage);
        for (Pair<String, String> entry : subscriptions) {
            Connection connection = connectionManager.getConnection(entry.getLeft());
            if (connection != null) {
                connection.send(msg);
            } else {
                log.warn("Not find connection for empty : {}", entry.getLeft());
            }
        }
    }

    class LastWillMessageEventListener implements EventListener {

        @Override
        public void onChange(MqttEvent event) {
            if (event.getEventType() == LAST_WILL_MESSAGE) {
                LastWillMessageEvent lwtEvent = (LastWillMessageEvent) event.getSourceEvent();
                if (!lwtEvent.getAddress().equals(advertisedAddress)) {
                    fireWillMessage("", lwtEvent.getWillMessage());
                }
            }
        }
    }
}
