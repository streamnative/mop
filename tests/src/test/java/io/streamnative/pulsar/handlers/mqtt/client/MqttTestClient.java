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
package io.streamnative.pulsar.handlers.mqtt.client;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.client.options.ConnectOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.Optional;
import io.streamnative.pulsar.handlers.mqtt.client.options.PublishOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.SubscribeOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.UnSubscribeOptions;
import java.util.concurrent.TimeUnit;

public interface MqttTestClient {

    Optional<MqttConnAckMessage> connect();

    Optional<MqttConnAckMessage> connect(ConnectOptions connectOptions);

    Optional<MqttSubAckMessage> subscribe(SubscribeOptions subscribeOptions);

    Optional<MqttPublishMessage> receive(long time, TimeUnit unit);

    Optional<MqttUnsubAckMessage> unsubscribe(UnSubscribeOptions unSubscribeOptions);

    Optional<MqttPubAckMessage> publish(PublishOptions publishOptions);

    Optional<MqttMessage> disconnect();
}
