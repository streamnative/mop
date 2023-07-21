package io.github.nmop;

import io.github.nmop.configuration.Configuration;
import lombok.Builder;
import lombok.Getter;
import org.apache.pulsar.broker.service.BrokerService;

@Builder
@Getter
public class HandlerContext {
    private final BrokerService brokerService;
    private final Configuration configuration;
}
