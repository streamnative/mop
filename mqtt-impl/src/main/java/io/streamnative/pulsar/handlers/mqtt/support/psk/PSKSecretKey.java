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

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.crypto.SecretKey;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class PSKSecretKey implements SecretKey {

    @Getter
    @Setter
    private String hint;

    @Getter
    private final String identity;
    private final String pwd;

    @Override
    public String getAlgorithm() {
        return "PSK";
    }

    @Override
    public String getFormat() {
        return "RAW";
    }

    @Override
    public byte[] getEncoded() {
        return pwd.getBytes(StandardCharsets.UTF_8);
    }

    public String getPlainText() {
        return String.format("%s:%s", identity, pwd);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PSKSecretKey that = (PSKSecretKey) o;
        return Objects.equals(identity, that.identity) && Objects.equals(pwd, that.pwd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identity, pwd);
    }
}
