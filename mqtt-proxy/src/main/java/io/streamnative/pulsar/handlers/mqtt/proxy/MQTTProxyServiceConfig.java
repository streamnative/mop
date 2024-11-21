package io.streamnative.pulsar.handlers.mqtt.proxy;


import lombok.Data;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Data
public class MQTTProxyServiceConfig {

    private MQTTProxyConfiguration proxyConfiguration;

    private MetadataStoreExtended localMetadataStore;

    private MetadataStoreExtended configMetadataStore;

    private AuthenticationService authenticationService;

    private PulsarClientImpl pulsarClient;

    private String bindAddress;

    private String advertisedAddress;
}
