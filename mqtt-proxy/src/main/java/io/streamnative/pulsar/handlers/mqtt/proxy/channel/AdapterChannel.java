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
package io.streamnative.pulsar.handlers.mqtt.proxy.channel;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.common.utils.FutureUtils;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class AdapterChannel {

    private final MQTTProxyAdapter adapter;
    @Getter
    private final InetSocketAddress broker;
    private CompletableFuture<Channel> channelFuture;

    public AdapterChannel(MQTTProxyAdapter adapter,
                          InetSocketAddress broker, CompletableFuture<Channel> channelFuture) {
        this.adapter = adapter;
        this.broker = broker;
        this.channelFuture = channelFuture;
    }

    public CompletableFuture<Void> writeAndFlush(final Connection connection, final MqttAdapterMessage adapterMsg) {
        checkArgument(StringUtils.isNotBlank(adapterMsg.getClientId()), "clientId is blank");
        final String clientId = adapterMsg.getClientId();
        adapterMsg.setEncodeType(MqttAdapterMessage.EncodeType.ADAPTER_MESSAGE);
        CompletableFuture<Void> future = channelFuture.thenCompose(channel -> {
            if (!channel.isActive()) {
                channelFuture = adapter.getChannel(broker);
                if (log.isDebugEnabled()) {
                    log.debug("channel is inactive, re-create channel to broker : {}", broker);
                }
                return writeConnectMessage(connection)
                        .thenCompose(__ -> writeAndFlush(connection, adapterMsg));
            }
            return FutureUtils.completableFuture(channel.writeAndFlush(adapterMsg));
        });
        future.exceptionally(ex -> {
            log.warn("[AdapterChannel][{}] Proxy write to broker {} failed."
                    + " adapterMsg message: {}", clientId, broker, adapterMsg, ex);
            return null;
        });
        return future;
    }

    private CompletableFuture<Void> writeConnectMessage(final Connection connection) {
        final MqttConnectMessage connectMessage = connection.getConnectMessage();
        return writeAndFlush(connection, new MqttAdapterMessage(connection.getClientId(), connectMessage));
    }

    /**
     * When client subscribes, the adapter channel maybe close in exception, so register listener to close the
     * related client channel and trigger reconnection.
     * @param connection
     */
    public void registerAdapterChannelInactiveListener(Connection connection) {
        channelFuture.thenAccept(channel -> {
            MQTTProxyAdapter.AdapterHandler channelHandler = (MQTTProxyAdapter.AdapterHandler)
                    channel.pipeline().get(MQTTProxyAdapter.AdapterHandler.NAME);
            channelHandler.registerAdapterChannelInactiveListener(connection);
        });
    }
}
