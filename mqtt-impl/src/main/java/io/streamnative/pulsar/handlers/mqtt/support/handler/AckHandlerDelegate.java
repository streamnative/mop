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
package io.streamnative.pulsar.handlers.mqtt.support.handler;

import io.streamnative.pulsar.handlers.mqtt.Connection;

public class AckHandlerDelegate {
    private final Connection connection;
    private final AckHandler delegate;

    private AckHandlerDelegate(Connection connection, AckHandler delegate) {
        this.connection = connection;
        this.delegate = delegate;
    }

    public static AckHandlerDelegate delegate(Connection connection, AckHandler handler) {
        return new AckHandlerDelegate(connection, handler);
    }

    public void connAck() {
        delegate.sendConnAck(connection);
    }


    public void connQosNotSupported() {
        delegate.sendConnNotSupportedAck(connection);
    }

    public void connClientIdentifierInvalid() {
        delegate.sendConnClientIdentifierInvalidAck(connection);
    }

    public void connAuthenticationFail() {
        delegate.sendConnAuthenticationFailAck(connection);
    }
}
