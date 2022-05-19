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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.ServerCnx;

/**
 * Server cnx for MQTT server.
 */
@Slf4j
public class MQTTServerCnx extends ServerCnx {

    public MQTTServerCnx(PulsarService pulsar, ChannelHandlerContext ctx) {
        super(pulsar);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
    }

    public ChannelHandlerContext ctx() {
        return this.ctx;
    }

    @Override
    protected void close() {
        super.close();
    }

    @Override
    public void closeConsumer(Consumer consumer) {
        safelyRemoveConsumer(consumer);
        MQTTConsumer mqttConsumer = (MQTTConsumer) consumer;
        mqttConsumer.getConnection().disconnect();
    }

    private void safelyRemoveConsumer(Consumer consumer) {
        long consumerId = consumer.consumerId();
        if (log.isDebugEnabled()) {
            log.debug("[{}] Removed consumer: consumerId={}, consumer={}", remoteAddress, consumerId, consumer);
        }
        CompletableFuture<Consumer> future = getConsumers().get(consumerId);
        if (future != null) {
            future.whenComplete((consumer2, exception) -> {
                if (exception != null || consumer2 == consumer) {
                    getConsumers().remove(consumerId, future);
                }
            });
        }
    }
}
