package io.streamnative.mop;

import static io.streamnative.mop.Envs.MQTT_PROTOCOL_NAME;
import static java.util.Objects.*;

import io.streamnative.mop.configuration.Configuration;
import io.streamnative.mop.configuration.Configurations;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ProtocolHandler implements org.apache.pulsar.broker.protocol.ProtocolHandler {
    private Configuration configuration;

    @Override
    public @Nonnull String protocolName() {
        return MQTT_PROTOCOL_NAME;
    }

    @Override
    public boolean accept(@Nonnull String protocol) {
        return MQTT_PROTOCOL_NAME.equalsIgnoreCase(protocol);
    }

    @Override
    public void initialize(@Nonnull ServiceConfiguration conf) {
        requireNonNull(conf);
        this.configuration = Configurations.parse(conf);
    }

    @Override
    public @Nonnull String getProtocolDataToAdvertise() {
        return "";
    }

    @Override
    public void start(@Nonnull BrokerService service) {
        requireNonNull(service);

    }

    @Override
    public @Nonnull Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        return new HashMap<>();
    }

    @Override
    public void close() {

    }
}
