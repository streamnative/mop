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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javax.net.ssl.SSLEngine;
import org.conscrypt.OpenSSLProvider;

public class PSKUtils {

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

    public static SSLEngine createClientEngine(SocketChannel ch, PSKConfiguration pskConfig) throws Exception {
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

    public static List<PSKSecretKey> parse(File file) {
        List<PSKSecretKey> result = new LinkedList<>();
        String line;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            while ((line = reader.readLine()) != null) {
                result.addAll(parse(line));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
        return result;
    }

    public static void write(File file, List<PSKSecretKey> pskKeys) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
            String identity = Joiner
                    .on(";")
                    .skipNulls()
                    .join(pskKeys.stream().map(key -> key.getPlainText()).collect(Collectors.toList()));
            writer.write(identity);
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    public static List<PSKSecretKey> parse(String identityText) {
        List<PSKSecretKey> result = new LinkedList<>();
        Iterable<String> split = Splitter.on(";").split(identityText);
        split.forEach(line -> {
            List<String> identityPwd = Splitter.on(":").limit(2).splitToList(line);
            String identity = identityPwd.get(0);
            String pwd = identityPwd.get(1);
            result.add(new PSKSecretKey(identity, pwd));
        });
        return result;
    }
}
