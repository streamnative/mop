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
package io.streamnative.pulsar.handlers.mqtt.proxy;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Configuration for MQTT proxy service.
 */
@Getter
@Setter
public class ProxyConfiguration {

    @Category
    private static final String CATEGORY_MQTT = "MQTT on Pulsar";
    @Category
    private static final String CATEGORY_MQTT_PROXY = "MQTT Proxy";
    @Category
    private static final String CATEGORY_BROKER_DISCOVERY = "Broker Discovery";
    @Category(
        description = "the settings are for configuring how proxies authenticates with Pulsar brokers"
    )
    private static final String CATEGORY_CLIENT_AUTHENTICATION = "Broker Client Authorization";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Mqtt on Pulsar Broker tenant"
    )
    private String mqttTenant = "public";

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
    private List<String> mqttAuthenticationMethods = ImmutableList.of();

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The maximum number of channels which can exist concurrently on a connection."
    )
    private int mqttMaxNoOfChannels = 64;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The maximum frame size on a connection."
    )
    private int mqttMaxFrameSize = 4 * 1024 * 1024;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The default heartbeat timeout on broker"
    )
    private int mqttHeartBeat = 60 * 1000;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "The mqtt proxy port"
    )
    private int mqttProxyPort = 5682;

    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            doc = "The service url points to the broker cluster"
    )
    private String brokerServiceURL;

    @FieldContext(
        category = CATEGORY_CLIENT_AUTHENTICATION,
        doc = "The authentication plugin used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientAuthenticationPlugin;
    @FieldContext(
        category = CATEGORY_CLIENT_AUTHENTICATION,
        doc = "The authentication parameters used by the Pulsar proxy to authenticate with Pulsar brokers"
    )
    private String brokerClientAuthenticationParameters;
}
