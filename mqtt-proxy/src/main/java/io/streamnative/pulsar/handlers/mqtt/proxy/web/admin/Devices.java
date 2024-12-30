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
package io.streamnative.pulsar.handlers.mqtt.proxy.web.admin;

import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.broker.web.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Path("/devices")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/devices", tags = "devices")
public class Devices extends WebResource {

    @GET
    @Path("/list")
    @ApiOperation(value = "List of connected devices.",
            response = String.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 500, message = "Internal server error")})
    public void getList(@Suspended final AsyncResponse asyncResponse) {
        try {
            final Collection<Connection> allConnections = service().getConnectionManager().getAllConnections();
            asyncResponse.resume(allConnections.stream().map(e ->
                    e.getClientId()).collect(Collectors.toList()));
        } catch (Exception e) {
            log.error("[{}] Failed to list devices {}", clientAppId(), e);
            asyncResponse.resume(new RestException(e));
        }

    }

    private static final Logger log = LoggerFactory.getLogger(Devices.class);
}
