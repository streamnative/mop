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
package io.streamnative.pulsar.handlers.mqtt.broker;

import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * MQTT on Pulsar service configuration object.
 */
@Getter
@Setter
public class MQTTServerConfiguration extends MQTTCommonConfiguration {

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Listener for the MQTT Server."
    )
    private String mqttListeners = "mqtt://127.0.0.1:1883";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Auto Set EventTime for pulsar message."
    )
    private boolean mqttAutoEventTime;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Fill eventtime from mqtt user properties."
    )
    private String mqttEventTimeFromProp = "";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Fill message key from mqtt user properties."
    )
    private String mqttMessageKeyFromProp = "";

}
