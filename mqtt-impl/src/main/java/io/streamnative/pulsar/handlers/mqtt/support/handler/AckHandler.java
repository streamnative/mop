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
package io.streamnative.pulsar.handlers.mqtt.support.handler;

import io.netty.channel.ChannelFuture;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.DisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.UnsubscribeAck;

/**
 * Ack handler.
 */
public interface AckHandler {

    ChannelFuture sendConnAck();

    ChannelFuture sendSubscribeAck(SubscribeAck subscribeAck);

    ChannelFuture sendUnsubscribeAck(UnsubscribeAck unsubscribeAck);

    ChannelFuture sendDisconnectAck(DisconnectAck disconnectAck);

    ChannelFuture sendPublishAck(PublishAck publishAck);

}
