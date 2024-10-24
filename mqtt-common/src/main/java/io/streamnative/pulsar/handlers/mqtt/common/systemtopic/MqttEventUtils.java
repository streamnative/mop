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
package io.streamnative.pulsar.handlers.mqtt.common.systemtopic;

import org.apache.bookkeeper.common.util.JsonUtil;

/**
 * Event message utils.
 */
public class MqttEventUtils {

    public static MqttEvent getMqttEvent(LastWillMessageEvent event, ActionType actionType) {
        MqttEvent.MqttEventBuilder builder = MqttEvent.builder();
        try {
            return builder
                    .key(event.getClientId() + "-LWT")
                    .eventType(EventType.LAST_WILL_MESSAGE)
                    .actionType(actionType)
                    .sourceEvent(JsonUtil.toJson(event))
                    .build();
        } catch (JsonUtil.ParseJsonException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static MqttEvent getMqttEvent(RetainedMessageEvent event, ActionType actionType) {
        MqttEvent.MqttEventBuilder builder = MqttEvent.builder();
        try {
            return builder
                    .key(event.getRetainedMessage().getTopic())
                    .eventType(EventType.RETAINED_MESSAGE)
                    .actionType(actionType)
                    .sourceEvent(JsonUtil.toJson(event))
                    .build();
        } catch (JsonUtil.ParseJsonException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static MqttEvent getMqttEvent(ConnectEvent event, ActionType actionType) {
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

    public static MqttEvent getMqttEvent(PSKEvent event, ActionType actionType) {
        MqttEvent.MqttEventBuilder builder = MqttEvent.builder();
        try {
            return builder
                    .key("add_psk_identity")
                    .eventType(EventType.ADD_PSK_IDENTITY)
                    .actionType(actionType)
                    .sourceEvent(JsonUtil.toJson(event))
                    .build();
        } catch (JsonUtil.ParseJsonException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
