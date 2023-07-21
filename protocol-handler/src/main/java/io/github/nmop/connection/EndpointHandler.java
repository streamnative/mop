package io.github.nmop.connection;

import io.github.nmop.HandlerContext;
import io.github.nmop.endpoint.Endpoint;
import io.github.nmop.endpoint.EndpointImpl;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

@Builder
@Slf4j
public final class EndpointHandler extends ChannelInboundHandlerAdapter {
    private final HandlerContext handlerContext;
    private volatile Endpoint endpoint;

    public EndpointHandler(@Nonnull HandlerContext handlerContext) {
        this.handlerContext = handlerContext;
    }

    @Override
    public void channelRead(@NotNull ChannelHandlerContext ctx, @NotNull Object msg) throws Exception {
        requireNonNull(ctx);
        requireNonNull(msg);
        if (!(msg instanceof MqttMessage mqttMessage)) {
            ctx.fireExceptionCaught(new UnsupportedMessageTypeException(msg.getClass()));
            return;
        }
        final var decoderResult = mqttMessage.decoderResult();
        if (decoderResult.isFailure()) {
            if (decoderResult.cause() instanceof MqttUnacceptableProtocolVersionException) {
                // todo reject connection
                return;
            }
            ctx.fireExceptionCaught(mqttMessage.decoderResult().cause());
            return;
        }
        if (!decoderResult.isFinished()) {
            ctx.fireExceptionCaught(new IllegalStateException("UNFINISHED Decoding"));
            return;
        }
        final var mqttMessageType = mqttMessage.fixedHeader().messageType();
        switch (mqttMessageType) {
            case CONNECT: {
                if (msg instanceof MqttConnectMessage connectMessage) {
                    if (endpoint != null) {
                        endpoint.close();
                        return;
                    }
                    var identifier = connectMessage.payload().clientIdentifier();
                    final var emptyIdentifier = identifier == null || identifier.isEmpty();
                    if (emptyIdentifier && handlerContext.getConfiguration().isAutoClientIdentifier()) {
                        identifier = UUID.randomUUID().toString();
                    }

                    this.endpoint = Endpoint.builder()
                            .ctx(ctx)
                            .build();

                    ctx.pipeline().remove("idle");
                    ctx.pipeline().remove("timeoutOnConnect");

                    if (connectMessage.variableHeader().keepAliveTimeSeconds() != 0) {
                        // the server waits for one and a half times the keep alive time period (MQTT spec)
                        // round to upper value to account for small keep-alive value (for testing)
                        int keepAliveTimeout = (int) Math.ceil(connectMessage.variableHeader().keepAliveTimeSeconds() * 1.5D);

                        // modifying the channel pipeline for adding the idle state handler with previous timeout
                        ctx.pipeline().addBefore("handler", "idle", new IdleStateHandler(keepAliveTimeout, 0, 0));
                        ctx.pipeline().addBefore("handler", "keepAliveHandler", new ChannelDuplexHandler() {

                            @Override
                            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

                                if (evt instanceof IdleStateEvent) {
                                    IdleStateEvent e = (IdleStateEvent) evt;
                                    if (e.state() == IdleState.READER_IDLE) {
                                        endpoint.close();
                                    }
                                }
                            }
                        }
                    }
                }
                case CONNACK:
                    break;
                case PUBLISH:
                    break;
                case PUBACK:
                    break;
                case PUBREC:
                    break;
                case PUBREL:
                    break;
                case PUBCOMP:
                    break;
                case SUBSCRIBE:
                    break;
                case SUBACK:
                    break;
                case UNSUBSCRIBE:
                    break;
                case UNSUBACK:
                    break;
                case PINGREQ:
                    break;
                case PINGRESP:
                    break;
                case DISCONNECT:
                    break;
                case AUTH:
                    break;
                default:
                    ctx.fireExceptionCaught(new UnsupportedMessageTypeException("Unsupported Mqtt message type:" + mqttMessageType));
            }
        }

        @Override
        public void channelActive (@NotNull ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive (@NotNull ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught (@Nonnull ChannelHandlerContext ctx, @Nonnull Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }

        @Override
        public void channelWritabilityChanged (@Nonnull ChannelHandlerContext ctx) throws Exception {
            super.channelWritabilityChanged(ctx);
        }
    }
