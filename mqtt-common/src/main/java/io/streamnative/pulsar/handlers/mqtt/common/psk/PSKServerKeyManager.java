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

package io.streamnative.pulsar.handlers.mqtt.common.psk;

import java.net.Socket;
import javax.crypto.SecretKey;
import javax.net.ssl.SSLEngine;
import org.conscrypt.PSKKeyManager;

/**
 * PSK server key manager.
 */
public class PSKServerKeyManager implements PSKKeyManager {

    private PSKConfiguration configuration;

    public PSKServerKeyManager(PSKConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public String chooseServerKeyIdentityHint(Socket socket) {
        return configuration.getIdentityHint();
    }

    @Override
    public String chooseServerKeyIdentityHint(SSLEngine engine) {
        return configuration.getIdentityHint();
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, Socket socket) {
        return null;
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, SSLEngine engine) {
        return null;
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, Socket socket) {
        return configuration.getKeyStore().getPSKSecretKey(identity);
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, SSLEngine engine) {
        return configuration.getKeyStore().getPSKSecretKey(identity);
    }
}
