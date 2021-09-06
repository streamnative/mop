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
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.net.URLEncoder;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for the {@link PulsarTopicUtils}.
 */
public class PulsarTopicUtilsTest {

    @Test
    public void testMqttTopicNameToPulsarTopicName() {
        String t0 = "/aaa/bbb/ccc";
        Assert.assertEquals(PulsarTopicUtils.getEncodedPulsarTopicName(t0, "public", "default", TopicDomain.persistent),
                "persistent://public/default/" + URLEncoder.encode(t0));
        t0 = "aaa/bbb/ccc/";
        Assert.assertEquals(PulsarTopicUtils.getEncodedPulsarTopicName(t0, "public", "default", TopicDomain.persistent),
                "persistent://public/default/" + URLEncoder.encode(t0));
        t0 = "/aaa/bbb/ccc/";
        Assert.assertEquals(PulsarTopicUtils.getEncodedPulsarTopicName(t0, "public", "default", TopicDomain.persistent),
                "persistent://public/default/" + URLEncoder.encode(t0));
        t0 = "persistent://public/default/aaa/bbb/ccc";
        Assert.assertEquals(PulsarTopicUtils.getEncodedPulsarTopicName(t0, "public", "default", TopicDomain.persistent),
                "persistent://public/default/" + URLEncoder.encode("aaa/bbb/ccc"));
        t0 = "persistent://public/default//aaa/bbb/ccc";
        Assert.assertEquals(PulsarTopicUtils.getEncodedPulsarTopicName(t0, "public", "default", TopicDomain.persistent),
                "persistent://public/default/" + URLEncoder.encode("/aaa/bbb/ccc"));
    }

    @Test
    public void testGetTopicFilter() {
        TopicFilter filter1 = PulsarTopicUtils.getTopicFilter("/a/b/c");
        TopicFilter filter2 = PulsarTopicUtils.getTopicFilter("persistent://public/default//a/b/c");
        Assert.assertEquals(filter1, filter2);
    }

    @Test
    public void testGetTopicDomainAndNamespaceFromTopicFilter() {
        Pair<TopicDomain, NamespaceName> pair1 = PulsarTopicUtils.getTopicDomainAndNamespaceFromTopicFilter(
                "/a/b/c", "public", "default", TopicDomain.persistent.value());
        Pair<TopicDomain, NamespaceName> pair2 = PulsarTopicUtils.getTopicDomainAndNamespaceFromTopicFilter(
                "persistent://public/default//a/b/c", "public", "default", TopicDomain.persistent.value());
        Assert.assertEquals(pair1, pair2);
    }

    @Test
    public void testGetToConsumerTopicName() {
        String mqttTopicname = "non-persistent://public/default/topicA";
        String pulsarTopicName = "non-persistent://public/default/topicA";
        String consumerTopicName1 = PulsarTopicUtils.getToConsumerTopicName(
                mqttTopicname, pulsarTopicName);
        Assert.assertEquals(mqttTopicname, consumerTopicName1);
        mqttTopicname = "topicA";
        String consumerTopicName2 = PulsarTopicUtils.getToConsumerTopicName(
                mqttTopicname, pulsarTopicName);
        Assert.assertEquals(mqttTopicname, consumerTopicName2);


        mqttTopicname = "persistent://public/default/topicA";
        pulsarTopicName = "persistent://public/default/topicA";
        consumerTopicName1 = PulsarTopicUtils.getToConsumerTopicName(
                mqttTopicname, pulsarTopicName);
        Assert.assertEquals(mqttTopicname, consumerTopicName1);
        mqttTopicname = "topicA";
        consumerTopicName2 = PulsarTopicUtils.getToConsumerTopicName(
                mqttTopicname, pulsarTopicName);
        Assert.assertEquals(mqttTopicname, consumerTopicName2);
    }

    @Test
    public void testAsyncGetTopicListFromTopicSubscriptionForDefaultTopicDomain() {
        //test default topic domain for  non-persistent
        List<String> topics1 = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                "/a/b/c", "public", "default", null, "non-persistent").join();
        List<String> topics2 = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                "non-persistent://public/default//a/b/c", "public", "default", null, "non-persistent").join();
        Assert.assertEquals(topics1, topics2);

        //test default topic domain for persistent
         topics1 = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                "/a/b/c", "public", "default", null, "persistent").join();
         topics2 = PulsarTopicUtils.asyncGetTopicListFromTopicSubscription(
                "persistent://public/default//a/b/c", "public", "default", null, "non-persistent").join();
        Assert.assertEquals(topics1, topics2);
    }


}
