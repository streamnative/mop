/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy handler.
 */
@Slf4j
public class MQTTProxyInboundHandler extends MQTTCommonInboundHandler {

    private final MQTTProxyService proxyService;

    public MQTTProxyInboundHandler(MQTTProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        processors.put("MQTTCommonInboundHandler", new MQTTProxyProtocolMethodProcessor(proxyService, ctx));
    }
}
