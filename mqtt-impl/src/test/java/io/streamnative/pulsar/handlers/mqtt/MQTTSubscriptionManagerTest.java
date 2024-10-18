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
package io.streamnative.pulsar.handlers.mqtt;

import com.google.common.collect.Lists;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.streamnative.pulsar.handlers.mqtt.broker.support.MQTTSubscriptionManager;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MQTTSubscriptionManagerTest {

    @Test
    public void testAddSubscriptions() {
        MQTTSubscriptionManager subscriptionManager = new MQTTSubscriptionManager();
        MqttTopicSubscription subscription1 = new MqttTopicSubscription("a/b/c", MqttQoS.AT_MOST_ONCE);
        Assert.assertFalse(subscriptionManager.addSubscriptions("client1", Lists.newArrayList(subscription1)));
        MqttTopicSubscription subscription2 = new MqttTopicSubscription("a/b/d", MqttQoS.AT_MOST_ONCE);
        Assert.assertFalse(subscriptionManager.addSubscriptions("client1", Lists.newArrayList(subscription2)));
    }

    @Test
    public void testAddDuplicatedSubscriptions() {
        MQTTSubscriptionManager subscriptionManager = new MQTTSubscriptionManager();
        MqttTopicSubscription subscription1 = new MqttTopicSubscription("a/b/c", MqttQoS.AT_MOST_ONCE);
        Assert.assertFalse(subscriptionManager.addSubscriptions("client1", Lists.newArrayList(subscription1)));
        MqttTopicSubscription subscription2 = new MqttTopicSubscription("a/b/c", MqttQoS.AT_MOST_ONCE);
        Assert.assertTrue(subscriptionManager.addSubscriptions("client1", Lists.newArrayList(subscription2)));
    }
}
