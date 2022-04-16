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
package io.streamnative.pulsar.handlers.mqtt.adapter;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import java.net.InetSocketAddress;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdapterChannel {

    private final MQTTProxyAdapter adapter;
    @Getter
    private final InetSocketAddress broker;
    private volatile Channel channel;

    public AdapterChannel(MQTTProxyAdapter adapter, InetSocketAddress broker, Channel channel) {
        this.adapter = adapter;
        this.broker = broker;
        this.channel = channel;
    }

    public ChannelFuture writeAndFlush(String clientId, MqttMessage msg) {
        MqttAdapterMessage adapterMessage = new MqttAdapterMessage(clientId, msg);
        adapterMessage.setAdapter(true);
        if (!channel.isActive()) {
            channel = adapter.getChannel(broker);
        }
        return channel.writeAndFlush(adapterMessage);
    }

    /**
     * When client subscribes, the adapter channel maybe close in exception, so register listener to close the
     * related client channel and trigger reconnection.
     * @param connection
     */
    public void registerAdapterChannelInactiveListener(Connection connection) {
        MQTTProxyAdapter.AdapterHandler channelHandler = (MQTTProxyAdapter.AdapterHandler)
                channel.pipeline().get("adapter-handler");
        channelHandler.registerAdapterChannelInactiveListener(connection);
    }

    public boolean isWritable() {
        return this.channel.isWritable();
    }

}
