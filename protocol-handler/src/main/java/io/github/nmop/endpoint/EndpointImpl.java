package io.github.nmop.endpoint;

import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
class EndpointImpl implements Endpoint {
    private final ChannelHandlerContext ctx;

    @Override
    public void close() throws Exception {

    }
}
