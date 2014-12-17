package org.nebulostore.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
@Path("/terminate")
public class ServerTerminationResource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ServerTerminationResource.class);
  private final RestModule restModule_;

  public ServerTerminationResource(RestModule restModule) {
    this.restModule_ = restModule;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response terminate() {
    LOGGER.info("Start method terminate()");
    restModule_.shutDown();
    LOGGER.info("End method terminate()");
    return Response.ok().build();
  }

}
