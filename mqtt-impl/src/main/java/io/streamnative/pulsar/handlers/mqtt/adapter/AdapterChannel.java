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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.utils.FutureUtils;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class AdapterChannel {

    private final MQTTProxyAdapter adapter;
    @Getter
    private final InetSocketAddress broker;
    private final Connection connection;
    private CompletableFuture<Channel> channelFuture;


    public AdapterChannel(MQTTProxyAdapter adapter, InetSocketAddress broker,
                          Connection connection, CompletableFuture<Channel> channelFuture) {
        this.adapter = adapter;
        this.broker = broker;
        this.connection = connection;
        this.channelFuture = channelFuture;
    }

    public CompletableFuture<Void> writeAndFlush(final MqttAdapterMessage adapterMsg) {
        return writeAndFlush(adapterMsg, 1);
    }

    public CompletableFuture<Void> writeAndFlush(final MqttAdapterMessage adapterMsg, int attempts) {
        checkArgument(StringUtils.isNotBlank(adapterMsg.getClientId()), "clientId is blank");
        final String clientId = adapterMsg.getClientId();
        adapterMsg.setEncodeType(MqttAdapterMessage.EncodeType.ADAPTER_MESSAGE);
        CompletableFuture<Void> future = channelFuture.thenCompose(channel -> {
            if (!channel.isActive()) {
                if (attempts > 5) {
                    throw new CompletionException(new MQTTServerException("Failed to reconnect adapter channel."));
                }
                channelFuture = adapter.getChannel(broker, connection);
                return writeAndFlush(adapterMsg, attempts + 1);
            }
            return FutureUtils.completableFuture(channel.writeAndFlush(adapterMsg));
        });
        future.exceptionally(ex -> {
            log.warn("[AdapterChannel][{}] Proxy write to broker {} failed."
                    + " error message: {}", clientId, broker, ex.getMessage());
            return null;
        });
        return future;
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
