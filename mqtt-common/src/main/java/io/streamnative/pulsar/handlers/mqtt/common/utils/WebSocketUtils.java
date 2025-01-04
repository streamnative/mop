/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.common.utils;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.streamnative.pulsar.handlers.mqtt.common.Constants;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.common.codec.MqttWebSocketCodec;

public class WebSocketUtils {

    /**
     * Add websocket handler.
     *
     * @param pipeline
     */
    public static void addWsHandler(ChannelPipeline pipeline, MQTTCommonConfiguration configuration) {
        // Encode or decode request and reply messages into HTTP messages
        pipeline.addLast(Constants.HANDLER_HTTP_CODEC, new HttpServerCodec());

        // Combine the parts of an HTTP message into a complete HTTP message
        pipeline.addLast(Constants.HANDLER_HTTP_AGGREGATOR,
                new HttpObjectAggregator(configuration.getHttpMaxContentLength()));

        // Compress and encode HTTP messages
        pipeline.addLast(Constants.HANDLER_HTTP_COMPRESSOR, new HttpContentCompressor());

        pipeline.addLast(Constants.HANDLER_WEB_SOCKET_SERVER_PROTOCOL,
                new WebSocketServerProtocolHandler(configuration.getWebSocketPath(),
                        Constants.MQTT_SUB_PROTOCOL_CSV_LIST, true, configuration.getWebSocketMaxFrameSize()));
        pipeline.addLast(Constants.HANDLER_MQTT_WEB_SOCKET_CODEC, new MqttWebSocketCodec());
    }
}
