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

import java.net.Socket;
import javax.crypto.SecretKey;
import javax.net.ssl.SSLEngine;
import org.conscrypt.PSKKeyManager;

/**
 * Psk client key manager.
 */
public class PSKClientKeyManager implements PSKKeyManager {

    protected PSKSecretKey secretKey;

    public PSKClientKeyManager(PSKSecretKey pskSecretKey) {
        this.secretKey = pskSecretKey;
    }

    @Override
    public String chooseServerKeyIdentityHint(Socket socket) {
        return secretKey.getHint();
    }

    @Override
    public String chooseServerKeyIdentityHint(SSLEngine engine) {
        return secretKey.getHint();
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, Socket socket) {
        return secretKey.getIdentity();
    }

    @Override
    public String chooseClientKeyIdentity(String identityHint, SSLEngine engine) {
        return secretKey.getIdentity();
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, Socket socket) {
        return secretKey;
    }

    @Override
    public SecretKey getKey(String identityHint, String identity, SSLEngine engine) {
        return secretKey;
    }
}
