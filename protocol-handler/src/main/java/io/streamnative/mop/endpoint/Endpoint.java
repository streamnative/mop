package io.streamnative.mop.endpoint;

public interface Endpoint extends AutoCloseable {

    static EndpointImpl.EndpointImplBuilder builder() {
        return EndpointImpl.builder();
    }
}
