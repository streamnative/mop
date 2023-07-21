package io.streamnative.mop.configuration;

import javax.annotation.Nonnull;

import lombok.experimental.UtilityClass;
import org.apache.pulsar.broker.ServiceConfiguration;

@UtilityClass
public final class Configurations {
    public static @Nonnull Configuration parse(
            @Nonnull ServiceConfiguration serviceConfiguration) {
        final var properties = serviceConfiguration.getProperties();
        final var listeners = (String) properties.getOrDefault("mqttListeners", "mqtt://127.0.0.1:1883");
        final var defaultNamespace = (String) properties.getOrDefault("mqttDefaultNamespace", "public/default");
        return Configuration.builder()
                .defaultNamespace(defaultNamespace)
                .listeners(listeners)
                .build();
    }
}
