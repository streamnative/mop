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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.streamnative.pulsar.handlers.mqtt.Constants;

/**
 * Some Netty's channels utilities.
 */
public final class NettyUtils {

    public static final String ATTR_USERNAME = "username";

    private static final AttributeKey<Object> ATTR_KEY_KEEPALIVE = AttributeKey.valueOf(Constants.KEEP_ALIVE);
    private static final AttributeKey<Object> ATTR_KEY_CLEANSESSION = AttributeKey.valueOf(Constants.CLEAN_SESSION);
    private static final AttributeKey<Object> ATTR_KEY_CLIENTID = AttributeKey.valueOf(Constants.ATTR_CLIENTID);
    private static final AttributeKey<Object> ATTR_KEY_USERNAME = AttributeKey.valueOf(ATTR_USERNAME);

    public static Object getAttribute(ChannelHandlerContext ctx, AttributeKey<Object> key) {
        Attribute<Object> attr = ctx.channel().attr(key);
        return attr.get();
    }

    public static void keepAlive(Channel channel, int keepAlive) {
        channel.attr(NettyUtils.ATTR_KEY_KEEPALIVE).set(keepAlive);
    }

    public static void cleanSession(Channel channel, boolean cleanSession) {
        channel.attr(NettyUtils.ATTR_KEY_CLEANSESSION).set(cleanSession);
    }

    public static boolean cleanSession(Channel channel) {
        return (Boolean) channel.attr(NettyUtils.ATTR_KEY_CLEANSESSION).get();
    }

    public static void clientID(Channel channel, String clientID) {
        channel.attr(NettyUtils.ATTR_KEY_CLIENTID).set(clientID);
    }

    public static String clientID(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_CLIENTID).get();
    }

    public static void userName(Channel channel, String username) {
        channel.attr(NettyUtils.ATTR_KEY_USERNAME).set(username);
    }

    public static String userName(Channel channel) {
        return (String) channel.attr(NettyUtils.ATTR_KEY_USERNAME).get();
    }

    private NettyUtils() {
    }
}
