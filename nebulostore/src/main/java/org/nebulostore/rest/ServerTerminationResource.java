package org.nebulostore.rest;

import java.util.concurrent.atomic.AtomicBoolean;

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
  private final AtomicBoolean isTerminate_;

  public ServerTerminationResource(AtomicBoolean isTerminate) {
    isTerminate_ = isTerminate;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response terminate() {
    LOGGER.info("Start method terminate()");
    synchronized (isTerminate_) {
      isTerminate_.set(true);
      isTerminate_.notify();
    }
    LOGGER.info("End method terminate()");
    return Response.ok().build();
  }

}
