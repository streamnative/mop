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
package io.streamnative.pulsar.handlers.mqtt.support.psk;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import javax.net.ssl.SSLEngine;
import lombok.extern.slf4j.Slf4j;
import org.conscrypt.OpenSSLProvider;

/**
 * PSK Engine factory.
 */
@Slf4j
public class PSKEngineFactory {

    public static SSLEngine createServerEngine(SocketChannel ch, PSKConfiguration pskConfig) throws Exception{
        SslContext sslContext = SslContextBuilder.forServer(new PSKServerKeyManager(pskConfig))
                .sslProvider(SslProvider.JDK)
                .sslContextProvider(new OpenSSLProvider())
                .applicationProtocolConfig(pskConfig.getProtocolConfig())
                .protocols(pskConfig.getProtocols())
                .ciphers(pskConfig.getCiphers())
                .build();
        SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
        sslEngine.setUseClientMode(false);
        return sslEngine;
    }

    public static SSLEngine createClientEngine(SocketChannel ch, PSKConfiguration pskConfig) throws Exception{
        SslContext sslContext = SslContextBuilder.forClient()
                .keyManager(new PSKClientKeyManager(pskConfig.getSecretKey()))
                .sslProvider(SslProvider.JDK)
                .sslContextProvider(new OpenSSLProvider())
                .applicationProtocolConfig(pskConfig.getProtocolConfig())
                .protocols(pskConfig.getProtocols())
                .ciphers(pskConfig.getCiphers())
                .build();
        SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
        sslEngine.setUseClientMode(true);
        return sslEngine;
    }
}
