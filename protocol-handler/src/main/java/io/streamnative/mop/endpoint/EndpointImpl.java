package io.streamnative.mop.endpoint;

import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
final class EndpointImpl implements Endpoint {
    private final ChannelHandlerContext ctx;

    @Override
    public void close() throws Exception {

    }
}
