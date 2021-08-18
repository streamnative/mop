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
package io.streamnative.pulsar.handlers.mqtt.untils;

import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for the {@link PulsarTopicUtils}.
 */
public class PulsarTopicUtilsTest {

    @Test
    public void testMqttTopicNameToPulsarTopicName() throws UnsupportedEncodingException {
        String t0 = "/aaa/bbb/ccc";
        Assert.assertEquals(PulsarTopicUtils.getPulsarTopicName(t0, "public", "default"),
                "persistent://public/default/" + URLEncoder.encode(t0));
        t0 = "aaa/bbb/ccc/";
        Assert.assertEquals(PulsarTopicUtils.getPulsarTopicName(t0, "public", "default"),
                "persistent://public/default/" + URLEncoder.encode(t0));
        t0 = "/aaa/bbb/ccc/";
        Assert.assertEquals(PulsarTopicUtils.getPulsarTopicName(t0, "public", "default"),
                "persistent://public/default/" + URLEncoder.encode(t0));
        t0 = "persistent://public/default/aaa/bbb/ccc";
        Assert.assertEquals(PulsarTopicUtils.getPulsarTopicName(t0, "public", "default"),
                "persistent://public/default/" + URLEncoder.encode("aaa/bbb/ccc"));
        t0 = "persistent://public/default//aaa/bbb/ccc";
        Assert.assertEquals(PulsarTopicUtils.getPulsarTopicName(t0, "public", "default"),
                "persistent://public/default/" + URLEncoder.encode("/aaa/bbb/ccc"));
    }
}
