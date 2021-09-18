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

import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class PSKUtils {

    public static List<PSKSecretKey> parse(File file) {
        List<PSKSecretKey> result = new LinkedList<>();
        String line;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
            while ((line = reader.readLine()) != null) {
                result.addAll(parse(line));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
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
