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
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CLIENT_ID;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CONNECT_MSG;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_TOPIC_SUBS;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;

/**
 * Some Netty's channels utilities.
 */
public final class NettyUtils {

    public static final String ATTR_USERNAME = "username";
    public static final String ATTR_USER_ROLE = "userRole";

    private static final AttributeKey<Object> ATTR_KEY_CLIENT_ID = AttributeKey.valueOf(ATTR_CLIENT_ID);
    private static final AttributeKey<Object> ATTR_KEY_USERNAME = AttributeKey.valueOf(ATTR_USERNAME);
    private static final AttributeKey<Object> ATTR_KEY_USER_ROLE = AttributeKey.valueOf(ATTR_USER_ROLE);
    private static final AttributeKey<Object> ATTR_KEY_CONNECT_MSG = AttributeKey.valueOf(ATTR_CONNECT_MSG);
    private static final AttributeKey<Object> ATTR_KEY_TOPIC_SUBS = AttributeKey.valueOf(ATTR_TOPIC_SUBS);
    private static final AttributeKey<Object> ATTR_KEY_CLIENT_ADDR = AttributeKey.valueOf(ATTR_CLIENT_ADDR);

    public static void attachClientID(Channel channel, String clientId) {
        channel.attr(NettyUtils.ATTR_KEY_CLIENT_ID).set(clientId);
    }

    public static void attachConnectMsg(Channel channel, MqttConnectMessage connectMessage) {
        channel.attr(NettyUtils.ATTR_KEY_CONNECT_MSG).set(connectMessage);
    }

    public static void attachTopicSubscriptions(Channel channel, Map<Topic, Pair<Subscription, Consumer>> topicSubs) {
        channel.attr(NettyUtils.ATTR_KEY_TOPIC_SUBS).set(topicSubs);
    }

    public static Map<Topic, Pair<Subscription, Consumer>> retrieveTopicSubscriptions(Channel channel) {
        return (Map<Topic, Pair<Subscription, Consumer>>) channel.attr(NettyUtils.ATTR_KEY_TOPIC_SUBS).get();
    }

    public static Optional<MqttConnectMessage> retrieveAndRemoveConnectMsg(Channel channel) {
        return Optional.ofNullable(channel.attr(NettyUtils.ATTR_KEY_CONNECT_MSG).getAndSet(null))
                .map(o -> (MqttConnectMessage) o);
    }

    public static String retrieveClientId(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_CLIENT_ID).get();
    }

    public static void userName(Channel channel, String username) {
        channel.attr(NettyUtils.ATTR_KEY_USERNAME).set(username);
    }

    public static String retrieveUserRole(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_USER_ROLE).get();
    }

    public static void attachUserRole(Channel channel, String authRole) {
        channel.attr(NettyUtils.ATTR_KEY_USER_ROLE).set(authRole);
    }

    public static String userName(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_USERNAME).get();
    }

    public static void addIdleStateHandler(Channel channel, int idleTime) {
        ChannelPipeline pipeline = channel.pipeline();
        if (pipeline.names().contains("idleStateHandler")) {
            pipeline.remove("idleStateHandler");
        }
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(idleTime, 0, 0));
    }

    public static String getAndAttachAddress(Channel channel) {
        String address = getAddress(channel);
        channel.attr(NettyUtils.ATTR_KEY_CLIENT_ADDR).set(address);
        return address;
    }

    public static String getAddress(Channel channel) {
        InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
        return address.getHostName() + ":" + address.getPort();
    }

    public static String retrieveAddress(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_CLIENT_ADDR).get();
    }

    private NettyUtils() {
    }
}
