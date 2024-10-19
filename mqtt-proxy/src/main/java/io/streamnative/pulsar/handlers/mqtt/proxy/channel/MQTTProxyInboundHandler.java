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
package io.streamnative.pulsar.handlers.mqtt.proxy.channel;

import static io.streamnative.pulsar.handlers.mqtt.common.Constants.DEFAULT_CLIENT_ID;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonInboundHandler;
import io.streamnative.pulsar.handlers.mqtt.proxy.impl.MQTTProxyProtocolMethodProcessor;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
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
        processors.put(DEFAULT_CLIENT_ID, new MQTTProxyProtocolMethodProcessor(proxyService, ctx));
    }
}
