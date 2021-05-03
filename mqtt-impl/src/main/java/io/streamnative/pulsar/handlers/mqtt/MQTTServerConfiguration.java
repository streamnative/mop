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

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * MQTT on Pulsar service configuration object.
 */
@Getter
@Setter
public class MQTTServerConfiguration extends ServiceConfiguration {

    @Category
    private static final String CATEGORY_MQTT = "MQTT on Pulsar";
    @Category
    private static final String CATEGORY_MQTT_PROXY = "MQTT Proxy";

    //
    // --- MQTT on Pulsar Broker configuration ---
    //

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Listener for the MQTT Server."
    )
    private String mqttListeners = "mqtt://127.0.0.1:1883";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Whether enable authentication for MQTT."
    )
    private boolean mqttAuthenticationEnabled = false;

    @FieldContext(
            category = CATEGORY_MQTT,
            doc = "A comma-separated list of authentication methods to enable."
    )
    private List<String> mqttAuthenticationMethods;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The maximum number of channels which can exist concurrently on a connection."
    )
    private int maxNoOfChannels = 64;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The default heartbeat timeout on broker"
    )
    private int heartBeat = 60 * 1000;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The maximum frame size on a connection."
    )
    private int maxFrameSize = 4 * 1024 * 1024;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Default Pulsar tenant that the MQTT server used."
    )
    private String defaultTenant = "public";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Default Pulsar namespace that the MQTT server used."
    )
    private String defaultNamespace = "default";

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "The mqtt proxy port"
    )
    private int mqttProxyPort = 5682;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "Whether start mqtt protocol handler with proxy"
    )
    private boolean mqttProxyEnable = false;


}
