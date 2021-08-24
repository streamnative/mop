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

import io.streamnative.pulsar.handlers.mqtt.TopicFilter;
import io.streamnative.pulsar.handlers.mqtt.TopicFilterImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TopicFilterImplTest {

    @Test
    public void testFilter() {
        TopicFilter filter = new TopicFilterImpl("a/b/c");
        Assert.assertFalse(filter.test("/a/b/c"));
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b/c/"));
        Assert.assertFalse(filter.test("/a/b/c/d"));
        Assert.assertFalse(filter.test("///"));
        Assert.assertFalse(filter.test("a/b/c/"));
        Assert.assertTrue(filter.test("a/b/c"));

        filter = new TopicFilterImpl("/a/b/c");
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b/c/"));
        Assert.assertFalse(filter.test("/a/b/c/d"));
        Assert.assertFalse(filter.test("///"));
        Assert.assertFalse(filter.test("a/b/c"));
        Assert.assertTrue(filter.test("/a/b/c"));

        filter = new TopicFilterImpl("a/b/c/");
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b/c/"));
        Assert.assertFalse(filter.test("/a/b/c/d"));
        Assert.assertFalse(filter.test("///"));
        Assert.assertFalse(filter.test("a/b/c"));
        Assert.assertFalse(filter.test("/a/b/c"));
        Assert.assertTrue(filter.test("a/b/c/"));

        filter = new TopicFilterImpl("/a/b/#");
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b"));
        Assert.assertFalse(filter.test("a/b"));
        Assert.assertFalse(filter.test("a/b/c"));
        Assert.assertTrue(filter.test("/a/b/c"));
        Assert.assertTrue(filter.test("/a/b/c/"));
        Assert.assertTrue(filter.test("/a/b/c/d"));
        Assert.assertTrue(filter.test("/a/b///"));

        filter = new TopicFilterImpl("a/b/#");
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b"));
        Assert.assertFalse(filter.test("a/b"));
        Assert.assertTrue(filter.test("a/b/c"));
        Assert.assertFalse(filter.test("/a/b/c"));
        Assert.assertFalse(filter.test("/a/b/c/"));
        Assert.assertFalse(filter.test("/a/b/c/d"));
        Assert.assertFalse(filter.test("/a/b///"));
        Assert.assertTrue(filter.test("a/b/c/"));
        Assert.assertTrue(filter.test("a/b/c/d"));
        Assert.assertTrue(filter.test("a/b///"));

        filter = new TopicFilterImpl("a/+/c");
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b"));
        Assert.assertFalse(filter.test("a/b"));
        Assert.assertTrue(filter.test("a/b/c"));
        Assert.assertFalse(filter.test("/a/b/c"));
        Assert.assertFalse(filter.test("/a/b/c/"));
        Assert.assertFalse(filter.test("/a/b/c/d"));
        Assert.assertFalse(filter.test("/a/b///"));
        Assert.assertFalse(filter.test("a/b/c/"));
        Assert.assertFalse(filter.test("a/b/c/d"));
        Assert.assertFalse(filter.test("a/b///"));

        filter = new TopicFilterImpl("/a/+/c");
        Assert.assertFalse(filter.test("/a"));
        Assert.assertFalse(filter.test("/a/b"));
        Assert.assertFalse(filter.test("a/b"));
        Assert.assertFalse(filter.test("a/b/c"));
        Assert.assertTrue(filter.test("/a/b/c"));
        Assert.assertFalse(filter.test("/a/b/c/"));
        Assert.assertFalse(filter.test("/a/b/c/d"));
        Assert.assertFalse(filter.test("/a/b///"));
        Assert.assertFalse(filter.test("a/b/c/"));
        Assert.assertFalse(filter.test("a/b/c/d"));
        Assert.assertFalse(filter.test("a/b///"));

    }
}
