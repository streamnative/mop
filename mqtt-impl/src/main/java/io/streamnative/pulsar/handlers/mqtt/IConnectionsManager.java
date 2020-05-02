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

import java.util.Collection;

/**
 * This interface will be used by an external codebase to retrieve and close physical connections.
 */
public interface IConnectionsManager {

    /**
     * Returns the number of physical connections.
     *
     * @return
     */
    int getActiveConnectionsNo();

    /**
     * Determines wether a MQTT client is connected to the broker.
     *
     * @param clientID
     * @return
     */
    boolean isConnected(String clientID);

    /**
     * Returns the identifiers of the MQTT clients that are connected to the broker.
     *
     * @return
     */
    Collection<String> getConnectedClientIds();

    /**
     * Closes a physical connection.
     *
     * @param clientID
     * @param closeImmediately
     *            If false, the connection will be flushed before it is closed.
     * @return
     */
    boolean closeConnection(String clientID, boolean closeImmediately);

}
