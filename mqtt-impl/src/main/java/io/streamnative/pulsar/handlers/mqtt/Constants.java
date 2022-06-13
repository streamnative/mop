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

import io.netty.handler.codec.mqtt.MqttProperties;

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

    public static final String USER_PROPERTY = MqttProperties.MqttPropertyType.USER_PROPERTY.name() + "_";

    public static final String RESPONSE_TOPIC = MqttProperties.MqttPropertyType.RESPONSE_TOPIC.name() + "_";

    public static final String CONTENT_TYPE = MqttProperties.MqttPropertyType.CONTENT_TYPE.name() + "_";

    public static final String CORRELATION_DATA = MqttProperties.MqttPropertyType.CORRELATION_DATA.name() + "_";


    private Constants() {
    }
}
