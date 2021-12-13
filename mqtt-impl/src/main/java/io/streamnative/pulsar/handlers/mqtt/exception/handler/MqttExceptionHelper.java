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
package io.streamnative.pulsar.handlers.mqtt.exception.handler;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTBadUserNameOrPasswordException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTClientIdentifierNotValidException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTDisconnectProtocolErrorException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTExceedServerReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNotAuthorizedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTProtocolVersionNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTQosNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerUnavailableException;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Map;

public class MqttExceptionHelper {

    private static final Map<Integer, MqttExceptionHandler> handlers = Maps.newConcurrentMap();

    static {
        MqttV5ExceptionHandler mqttV5ExceptionHandler = new MqttV5ExceptionHandler();
        MqttV3xExceptionHandler mqttV3xExceptionHandler = new MqttV3xExceptionHandler();
        handlers.put((int) MqttVersion.MQTT_5.protocolLevel(), mqttV5ExceptionHandler);
        handlers.put((int) MqttVersion.MQTT_3_1.protocolLevel(), mqttV3xExceptionHandler);
        handlers.put((int) MqttVersion.MQTT_3_1_1.protocolLevel(), mqttV3xExceptionHandler);
    }

    private static MqttExceptionHandler getExceptionHandler(Channel channel) {
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        MqttExceptionHandler exceptionHandler = handlers.get(protocolVersion);
        if (exceptionHandler == null) {
            throw new MQTTServerException("unsupported protocol version");
        }
        return exceptionHandler;
    }

    public static void handleCommonException(MqttMessageType condition, Channel channel, int packetId, Throwable ex) {
        MqttExceptionHandler exceptionHandler = getExceptionHandler(channel);
        switch (condition) {
            case PUBACK:
                exceptionHandler.handlePubCommonException(channel, packetId, ex);
                break;
            case SUBACK:
                exceptionHandler.handleSubCommonException(channel, packetId, ex);
                break;
            case UNSUBACK:
                exceptionHandler.handleUnSubCommonException(channel, packetId, ex);
                break;
            default:
                throw new IllegalArgumentException(String.format("Illegal common exception handler type %s.",
                        condition));
        }
    }

    public static boolean handleMqttServerException(Channel channel, MQTTServerException ex) {
        MqttExceptionHandler exceptionHandler = getExceptionHandler(channel);
        if (ex instanceof MQTTBadUserNameOrPasswordException) {
            exceptionHandler.handleConnBadUserNameOrPassword(channel,
                    (MQTTBadUserNameOrPasswordException) ex);
            return true;
        } else if (ex instanceof MQTTProtocolVersionNotSupportException) {
            exceptionHandler.handleProtocolVersionNotSupport(channel, (MQTTProtocolVersionNotSupportException) ex);
            return true;
        } else if (ex instanceof MQTTClientIdentifierNotValidException) {
            exceptionHandler.handleConnClientIdentifierNotValid(channel,
                    (MQTTClientIdentifierNotValidException) ex);
            return true;
        } else if (ex instanceof MQTTDisconnectProtocolErrorException) {
            exceptionHandler.handleDisconnectionProtocolError(channel, (MQTTDisconnectProtocolErrorException) ex);
            return true;
        } else if (ex instanceof MQTTExceedServerReceiveMaximumException) {
            exceptionHandler.handlePubExceedServerMaximumReceive(channel,
                    (MQTTExceedServerReceiveMaximumException) ex);
            return true;
        } else if (ex instanceof MQTTNoMatchingSubscriberException) {
            exceptionHandler.handlePubNoMatchingSubscriber(channel, (MQTTNoMatchingSubscriberException) ex);
            return true;
        } else if (ex instanceof MQTTNoSubscriptionExistedException) {
            exceptionHandler.handleUnSubNoSubscriptionExisted(channel, (MQTTNoSubscriptionExistedException) ex);
            return true;
        } else if (ex instanceof MQTTNotAuthorizedException) {
            MqttMessageType condition = ((MQTTNotAuthorizedException) ex).getCondition();
            switch (condition) {
                case SUBACK:
                    exceptionHandler.handleSubNotAuthorized(channel, (MQTTNotAuthorizedException) ex);
                    return true;
                case PUBACK:
                    exceptionHandler.handlePubNotAuthorized(channel, (MQTTNotAuthorizedException) ex);
                    return true;
                default:
                    return false;
            }
        } else if (ex instanceof MQTTQosNotSupportException) {
            exceptionHandler.handleConnQosNotSupport(channel, (MQTTQosNotSupportException) ex);
            return true;
        } else if (ex instanceof MQTTServerUnavailableException) {
            exceptionHandler.handleConnServerUnavailable(channel, (MQTTServerUnavailableException) ex);
            return true;
        }
        return false;
    }

}