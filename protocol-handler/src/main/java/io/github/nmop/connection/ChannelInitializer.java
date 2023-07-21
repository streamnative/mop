package io.github.nmop.connection;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

public final class ChannelInitializer extends io.netty.channel.ChannelInitializer<SocketChannel> {
    private final BrokerService brokerService;

    public ChannelInitializer(@Nonnull BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    protected void initChannel(@NotNull SocketChannel channel) {
        final var pipeline = channel.pipeline();
        pipeline.addLast("mqttEncoder", MqttEncoder.INSTANCE);
        pipeline.addLast("mqttDecoder", new MqttDecoder());
        pipeline.addLast("idle", new IdleStateHandler(90, 0, 0));
        pipeline.addLast("timeoutOnConnect", new ChannelDuplexHandler() {

            @Override
            public void userEventTriggered(@Nonnull ChannelHandlerContext ctx, @Nonnull Object event) throws Exception {
                requireNonNull(ctx);
                requireNonNull(event);
                if (event instanceof IdleStateEvent) {
                    final var e = (IdleStateEvent) event;
                    if (e.state() == IdleState.READER_IDLE) {
                        //  If the Server does not receive a CONNECT Packet within a reasonable amount of
                        //  time after the Network Connection is established, the Server SHOULD close the connection.
                        //  [MQTT-3.1.4-1]
                        ctx.channel().close();
                    }
                }
                super.userEventTriggered(ctx, event);
            }
        });
        final var connection = EndpointHandler.builder().brokerService(brokerService).build();
        pipeline.addLast("endpointHandler", connection);
    }
}
