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

import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CLEAN_SESSION;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CLIENT_ADDR;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CLIENT_ID;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CONNECTION;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_CONNECT_MSG;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_KEEP_ALIVE_TIME;
import static io.streamnative.pulsar.handlers.mqtt.Constants.ATTR_TOPIC_SUBS;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.streamnative.pulsar.handlers.mqtt.Connection;
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
    private static final AttributeKey<Object> ATTR_KEY_CONNECTION = AttributeKey.valueOf(ATTR_CONNECTION);
    private static final AttributeKey<Object> ATTR_KEY_CLEAN_SESSION = AttributeKey.valueOf(ATTR_CLEAN_SESSION);
    private static final AttributeKey<Object> ATTR_KEY_KEEP_ALIVE_TIME = AttributeKey.valueOf(ATTR_KEEP_ALIVE_TIME);
    private static final AttributeKey<Object> ATTR_KEY_USERNAME = AttributeKey.valueOf(ATTR_USERNAME);
    private static final AttributeKey<Object> ATTR_KEY_USER_ROLE = AttributeKey.valueOf(ATTR_USER_ROLE);
    private static final AttributeKey<Object> ATTR_KEY_CONNECT_MSG = AttributeKey.valueOf(ATTR_CONNECT_MSG);
    private static final AttributeKey<Object> ATTR_KEY_TOPIC_SUBS = AttributeKey.valueOf(ATTR_TOPIC_SUBS);
    private static final AttributeKey<Object> ATTR_KEY_CLIENT_ADDR = AttributeKey.valueOf(ATTR_CLIENT_ADDR);

    public static void setClientId(Channel channel, String clientId) {
        channel.attr(NettyUtils.ATTR_KEY_CLIENT_ID).set(clientId);
    }

    public static String getClientId(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_CLIENT_ID).get();
    }

    public static void setConnection(Channel channel, Connection connection) {
        channel.attr(NettyUtils.ATTR_KEY_CONNECTION).set(connection);
    }

    public static Connection getConnection(Channel channel) {
        return (Connection) channel.attr(NettyUtils.ATTR_KEY_CONNECTION).get();
    }

    public static void setCleanSession(Channel channel, boolean cleanSession) {
        channel.attr(NettyUtils.ATTR_KEY_CLEAN_SESSION).set(cleanSession);
    }

    public static boolean getCleanSession(Channel channel) {
        return (Boolean) channel.attr(NettyUtils.ATTR_KEY_CLEAN_SESSION).get();
    }

    public static void setConnectMsg(Channel channel, MqttConnectMessage connectMessage) {
        channel.attr(NettyUtils.ATTR_KEY_CONNECT_MSG).set(connectMessage);
    }

    public static void setTopicSubscriptions(Channel channel, Map<Topic, Pair<Subscription, Consumer>> topicSubs) {
        channel.attr(NettyUtils.ATTR_KEY_TOPIC_SUBS).set(topicSubs);
    }

    public static Map<Topic, Pair<Subscription, Consumer>> getTopicSubscriptions(Channel channel) {
        return (Map<Topic, Pair<Subscription, Consumer>>) channel.attr(NettyUtils.ATTR_KEY_TOPIC_SUBS).get();
    }

    public static Optional<MqttConnectMessage> getAndRemoveConnectMsg(Channel channel) {
        return Optional.ofNullable(channel.attr(NettyUtils.ATTR_KEY_CONNECT_MSG).getAndSet(null))
                .map(o -> (MqttConnectMessage) o);
    }

    public static MqttConnectMessage getConnectMsg(Channel channel) {
        return (MqttConnectMessage) channel.attr(NettyUtils.ATTR_KEY_CONNECT_MSG).get();
    }

    public static void userName(Channel channel, String username) {
        channel.attr(NettyUtils.ATTR_KEY_USERNAME).set(username);
    }

    public static String getUserRole(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_USER_ROLE).get();
    }

    public static void setUserRole(Channel channel, String authRole) {
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
        pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTime));
    }

    public static void setKeepAliveTime(Channel channel, int keepAliveTime) {
        channel.attr(NettyUtils.ATTR_KEY_KEEP_ALIVE_TIME).set(keepAliveTime);
    }

    public static int getKeepAliveTime(Channel channel) {
        return (Integer) channel.attr(NettyUtils.ATTR_KEY_KEEP_ALIVE_TIME).get();
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
