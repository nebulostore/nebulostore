package org.nebulostore.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.JsonObject;
import com.google.inject.Inject;

import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.utils.JSONFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
@Path("network_monitor/")
public class NetworkMonitorResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(NetworkMonitorResource.class);
  private NetworkMonitor networkMonitor_;

  @Inject
  public NetworkMonitorResource(NetworkMonitor networkMonitor) {
    networkMonitor_ = networkMonitor;
  }


  @GET
  @Path("peers_list")
  @Produces(MediaType.APPLICATION_JSON)
  public String getPeersList() {
    LOGGER.info("Start method getPeersList()");
    JsonObject result = JSONFactory.convertFromList(networkMonitor_.getKnownPeers());
    LOGGER.info(result.toString());
    LOGGER.info("End method getPeersList()");
    return result.toString();
  }
}
