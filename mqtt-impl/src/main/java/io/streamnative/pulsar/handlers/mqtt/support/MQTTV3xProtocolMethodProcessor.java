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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.annotation.Ignore;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;

public class MQTTV3xProtocolMethodProcessor extends AbstractProtocolMethodProcessor {

    public MQTTV3xProtocolMethodProcessor(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService, ctx);
    }


    @Override
    Connection buildConnection(Connection.ConnectionBuilder connectionBuilder, MqttConnectMessage msg) {
        connectionBuilder
                .clientReceiveMaximum(MqttPropertyUtils.BEFORE_DEFAULT_RECEIVE_MAXIMUM);
        return connectionBuilder.build();
    }

    @Override
    @Ignore
    void checkWillingMessageIfNeeded(String clientId, int willQos) {
    }

    @Override
    @Ignore
    void checkServerReceivePubMessageAndIncrementCounterIfNeeded(Channel channel, MqttPublishMessage msg) {
    }

    @Override
    @Ignore
    void parseDisconnectPropertiesIfNeeded(MqttMessage msg) {
    }
}
