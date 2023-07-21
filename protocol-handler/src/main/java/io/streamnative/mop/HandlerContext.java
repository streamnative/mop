package io.streamnative.mop;

import io.streamnative.mop.configuration.Configuration;
import lombok.Builder;
import lombok.Getter;
import org.apache.pulsar.broker.service.BrokerService;

@Builder
@Getter
public final class HandlerContext {
    private final BrokerService brokerService;
    private final Configuration configuration;
}
