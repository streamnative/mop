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
package io.streamnative.pulsar.handlers.mqtt.common.messages;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.streamnative.pulsar.handlers.mqtt.common.exception.restrictions.InvalidReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.common.mqtt5.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Mqtt property utils.
 * @see <a>https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.pdf</a>
 */
@Slf4j
public class MqttPropertyUtils {

    /**
     * Get session expire interval.
     * @param properties - mqtt properties
     * @return Integer - expire interval value
     */
    @SuppressWarnings("unchecked")
    public static Optional<Integer> getExpireInterval(MqttProperties properties) {
        MqttProperties.MqttProperty<Integer> property = properties
                .getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        if (property == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(property.value());
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> getProperty(MqttProperties properties, MqttProperties.MqttPropertyType type) {
        MqttProperties.MqttProperty<T> property = properties.getProperty(type.value());
        if (property == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(property.value());
    }

    public static void parsePropertiesToStuffRestriction(
            ClientRestrictions.ClientRestrictionsBuilder clientRestrictionsBuilder,
            MqttConnectMessage connectMessage)
            throws InvalidReceiveMaximumException {
        MqttProperties properties = connectMessage.variableHeader().properties();
        // parse expire interval
        getExpireInterval(properties)
                .ifPresent(clientRestrictionsBuilder::sessionExpireInterval);
        // parse receive maximum
        Optional<Integer> receiveMaximum = getProperty(properties, MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM);
        if (receiveMaximum.isPresent() && receiveMaximum.get() == 0) {
            throw new InvalidReceiveMaximumException("Not Allow Receive maximum property value zero");
        } else {
            receiveMaximum.ifPresent(clientRestrictionsBuilder::receiveMaximum);
        }
        // parse maximum packet size
        Optional<Integer> maximumPacketSize = getProperty(properties, MqttProperties
                .MqttPropertyType.MAXIMUM_PACKET_SIZE);
        maximumPacketSize.ifPresent(clientRestrictionsBuilder::maximumPacketSize);
        // parse request problem information
        Optional<Integer> requestProblemInformation =
                getProperty(properties, MqttProperties.MqttPropertyType.REQUEST_PROBLEM_INFORMATION);
        // the empty option means allowing reason string or user property
        clientRestrictionsBuilder.allowReasonStrOrUserProperty(!requestProblemInformation.isPresent());
        Optional<Integer> topicAliasMaximum =
                getProperty(properties, MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM);
        topicAliasMaximum.ifPresent(clientRestrictionsBuilder::topicAliasMaximum);
    }

    /**
     * Stuff reason string to mqtt property.
     *
     * @param properties   Mqtt properties
     * @param reasonString reason string
     */
    public static void setReasonString(MqttProperties properties, String reasonString) {
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        reasonString);
        properties.add(reasonStringProperty);
    }

    public static Optional<Integer> getUpdateSessionExpireIntervalIfExist(int protocolVersion, MqttMessage msg) {
        if (MqttUtils.isNotMqtt3(protocolVersion)
                && msg.variableHeader() instanceof MqttReasonCodeAndPropertiesVariableHeader) {
            return MqttPropertyUtils
                    .getExpireInterval(((MqttReasonCodeAndPropertiesVariableHeader)
                            msg.variableHeader()).properties());
        } else {
            return Optional.empty();
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getUserProperties(MqttProperties properties) {
        List<MqttProperties.UserProperty> userProperties = (List<MqttProperties.UserProperty>) properties
                .getProperties(MqttProperties.MqttPropertyType.USER_PROPERTY.value());
        return userProperties.stream()
                .collect(Collectors.toMap(v -> v.value().key, v -> v.value().value));
    }
}
