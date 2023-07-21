package io.github.nmop.endpoint;

public interface Endpoint extends AutoCloseable {

    static EndpointImpl.EndpointImplBuilder builder() {
        return EndpointImpl.builder();
    }
}
