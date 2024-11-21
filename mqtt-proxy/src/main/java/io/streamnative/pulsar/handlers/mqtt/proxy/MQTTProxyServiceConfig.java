package io.streamnative.pulsar.handlers.mqtt.proxy;


import lombok.Data;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.metadata.api.MetadataStore;

@Data
public class MQTTProxyServiceConfig {

    private MQTTProxyConfiguration proxyConfiguration;

    private MetadataStore localMetadataStore;

    private MetadataStore configMetadataStore;

    private AuthenticationService authenticationService;

    private PulsarClient pulsarClient;

    private String bindAddress;

    private String advertisedAddress;
}
