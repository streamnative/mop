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
package io.streamnative.pulsar.handlers.mqtt.utils;

import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CLIENT_ADDR;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CONNECTION;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_TOPIC_SUBS;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;

/**
 * Some Netty's channels utilities.
 */
public final class NettyUtils {

    public static final AttributeKey<Object> ATTR_KEY_CONNECTION = AttributeKey.valueOf(ATTR_CONNECTION);
    private static final AttributeKey<Object> ATTR_KEY_TOPIC_SUBS = AttributeKey.valueOf(ATTR_TOPIC_SUBS);
    private static final AttributeKey<Object> ATTR_KEY_CLIENT_ADDR = AttributeKey.valueOf(ATTR_CLIENT_ADDR);

    public static Connection getConnection(Channel channel) {
        return (Connection) channel.attr(NettyUtils.ATTR_KEY_CONNECTION).get();
    }

    public static void setTopicSubscriptions(Channel channel, Map<Topic, Pair<Subscription, Consumer>> topicSubs) {
        channel.attr(NettyUtils.ATTR_KEY_TOPIC_SUBS).set(topicSubs);
    }

    public static Map<Topic, Pair<Subscription, Consumer>> getTopicSubscriptions(Channel channel) {
        return (Map<Topic, Pair<Subscription, Consumer>>) channel.attr(NettyUtils.ATTR_KEY_TOPIC_SUBS).get();
    }

    public static String getAndSetAddress(Channel channel) {
        String address = getRemoteAddress(channel);
        channel.attr(NettyUtils.ATTR_KEY_CLIENT_ADDR).set(address);
        return address;
    }

    private static String getRemoteAddress(Channel channel) {
        InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
        return address.getHostName() + ":" + address.getPort();
    }

    public static String getAddress(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_CLIENT_ADDR).get();
    }

    private NettyUtils() {
    }
}
