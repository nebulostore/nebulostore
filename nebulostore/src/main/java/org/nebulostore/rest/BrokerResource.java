package org.nebulostore.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.JsonObject;
import com.google.inject.Inject;

import org.nebulostore.broker.BrokerContext;
import org.nebulostore.utils.JSONFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
@Path("broker/")
public class BrokerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResource.class);
  private final BrokerContext brokerContext_;

  @Inject
  public BrokerResource(BrokerContext brokerContext) {
    brokerContext_ = brokerContext;
  }


  @GET
  @Path("contract_list")
  @Produces(MediaType.APPLICATION_JSON)
  public String getContractList() {
    LOGGER.info("Start method getContractList()");
    JsonObject result = JSONFactory.convertFromMap(brokerContext_.getContractList());
    LOGGER.info(result.toString());
    LOGGER.info("End method getContractList()");
    return result.toString();
  }
}
