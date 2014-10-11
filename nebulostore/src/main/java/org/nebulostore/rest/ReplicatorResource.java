package org.nebulostore.rest;

import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.gson.JsonElement;
import com.google.inject.Inject;
import com.google.inject.Provider;

import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.utils.JSONFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
@Path("replicator/")
public class ReplicatorResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ReplicatorResource.class);
  private Provider<Replicator> replicatorProvider_;

  @Inject
  public ReplicatorResource(Provider<Replicator> replicatorProvider) {
    replicatorProvider_ = replicatorProvider;
  }


  @GET
  @Path("files_list")
  @Produces(MediaType.APPLICATION_JSON)
  public String getFilesList() {
    LOGGER.info("Start method getFilesList()");
    Set<String> objectsId = replicatorProvider_.get().getStoredObjectsIds();
    JsonElement result = JSONFactory.convertFromCollection(objectsId);
    LOGGER.info(result.toString());
    LOGGER.info("End method getFilesList()");
    return result.toString();
  }

  @GET
  @Path("file_metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public String getFileMeta() {
    return "{}";
  }
}
