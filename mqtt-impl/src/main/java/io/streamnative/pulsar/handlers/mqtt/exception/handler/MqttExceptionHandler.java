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


import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTBadUserNameOrPasswordException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTClientIdentifierNotValidException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTDisconnectProtocolErrorException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTExceedServerReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNotAuthorizedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTProtocolVersionNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTQosNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerUnavailableException;

public interface MqttExceptionHandler {

    void handleProtocolVersionNotSupport(Channel channel, MQTTProtocolVersionNotSupportException ex);

    void handleConnClientIdentifierNotValid(Channel channel, MQTTClientIdentifierNotValidException ex);

    void handleConnBadUserNameOrPassword(Channel channel, MQTTBadUserNameOrPasswordException ex);

    void handleConnQosNotSupport(Channel channel, MQTTQosNotSupportException ex);

    void handleConnServerUnavailable(Channel channel, MQTTServerUnavailableException ex);

    void handlePubNotAuthorized(Channel channel, MQTTNotAuthorizedException ex);

    void handlePubExceedServerMaximumReceive(Channel channel, MQTTExceedServerReceiveMaximumException ex);

    void handlePubNoMatchingSubscriber(Channel channel, MQTTNoMatchingSubscriberException ex);

    void handleDisconnectionProtocolError(Channel channel, MQTTDisconnectProtocolErrorException ex);

    void handleSubNotAuthorized(Channel channel, MQTTNotAuthorizedException ex);

    void handleUnSubNoSubscriptionExisted(Channel channel, MQTTNoSubscriptionExistedException exception);

    void handleUnSubCommonException(Channel channel, int packetId, Throwable ex);

    void handleSubCommonException(Channel channel, int packetId, Throwable ex);

    void handlePubCommonException(Channel channel, int packetId, Throwable e);

}
