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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.support.ProtocolMethodProcessorImpl;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

/**
 * A channel initializer that initialize channels for MQTT protocol.
 */
public class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final MQTTServerConfiguration mqttConfig;

    public MQTTChannelInitializer(PulsarService pulsarService,
                                  MQTTServerConfiguration mqttConfig)
            throws Exception {
        super();
        this.pulsarService = pulsarService;
        this.mqttConfig = mqttConfig;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(10, 0, 0));
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler",
                new MQTTInboundHandler(new ProtocolMethodProcessorImpl(pulsarService, mqttConfig)));
    }
}
