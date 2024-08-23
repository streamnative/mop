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

/**
 * Server constants keeper.
 */
public final class Constants {

    public static final String DEFAULT_CLIENT_ID = "__MoPInternalClientId";
    public static final String ATTR_CONNECTION = "Connection";
    public static final String ATTR_CLIENT_ADDR = "ClientAddr";
    public static final String AUTH_BASIC = "basic";
    public static final String AUTH_TOKEN = "token";

    public static final String ATTR_TOPIC_SUBS = "topicSubs";

    public static final String MQTT_PROPERTIES = "MQTT_PROPERTIES_%d_";

    public static final String MQTT_PROPERTIES_PREFIX = "MQTT_PROPERTIES_";

    /**
     * netty handler name constant.
     */
    public static final String HANDLER_HTTP_CODEC = "httpCodecHandler";

    public static final String HANDLER_HTTP_AGGREGATOR = "httpAggregatorHandler";

    public static final String HANDLER_HTTP_COMPRESSOR = "httpCompressorHandler";

    public static final String HANDLER_WEB_SOCKET_SERVER_PROTOCOL = "webSocketServerProtocolHandler";

    public static final String HANDLER_MQTT_WEB_SOCKET_CODEC = "mqttWebSocketCodecHandler";

    public static final String MQTT_SUB_PROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1, mqttv5.0";

    public static final String MTLS = "mTls";

    private Constants() {
    }
}
