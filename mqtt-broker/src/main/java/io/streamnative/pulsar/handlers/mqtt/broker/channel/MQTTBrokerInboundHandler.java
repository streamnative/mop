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
package io.streamnative.pulsar.handlers.mqtt.broker.channel;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.broker.processor.MQTTBrokerProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonInboundHandler;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterMessage;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT in bound handler.
 */
@Sharable
@Slf4j
public class MQTTBrokerInboundHandler extends MQTTCommonInboundHandler {

    public static final String NAME = "handler";

    private final MQTTService mqttService;

    public MQTTBrokerInboundHandler(MQTTService mqttService) {
        this.mqttService = mqttService;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        checkArgument(message instanceof MqttAdapterMessage);
        MqttAdapterMessage adapterMsg = (MqttAdapterMessage) message;
        log.info("broker channel read from client : {}", adapterMsg.getClientId());
        processors.computeIfAbsent(adapterMsg.getClientId(), key -> {
            MQTTBrokerProtocolMethodProcessor p = new MQTTBrokerProtocolMethodProcessor(mqttService, ctx);
            CompletableFuture<Void> inactiveFuture = p.getInactiveFuture();
            inactiveFuture.whenComplete((id, ex) -> {
                processors.remove(adapterMsg.getClientId());
            });
            return p;
        });
        super.channelRead(ctx, message);
    }
}
