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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * PSK secret key store.
 */
public class PSKSecretKeyStore {

    private final ConcurrentMap<String, PSKSecretKey> secretKeyMap = new ConcurrentHashMap<>();

    public void addPSKSecretKey(PSKSecretKey secretKey) {
        secretKeyMap.put(secretKey.getIdentity(), secretKey);
    }

    public void addPSKSecretKey(String key, String identity) {
        addPSKSecretKey(new PSKSecretKey(key, identity));
    }

    public PSKSecretKey getPSKSecretKey(String key) {
        return secretKeyMap.get(key);
    }
}
