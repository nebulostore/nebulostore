package org.nebulostore.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.nebulostore.peers.AbstractPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
@Path("/terminate")
public class ServerTerminationResource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ServerTerminationResource.class);
  private final AbstractPeer abstractPeer_;

  @Inject
  public ServerTerminationResource(AbstractPeer abstractPeer) {
    this.abstractPeer_ = abstractPeer;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response terminate() {
    LOGGER.info("Start method terminate()");
    abstractPeer_.quitNebuloStore();
    LOGGER.info("Finished peer.quitNebulostore()");
    return Response.ok().build();
  }

}
