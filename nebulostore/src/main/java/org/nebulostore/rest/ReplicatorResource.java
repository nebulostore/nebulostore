package org.nebulostore.rest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.io.ByteStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.inject.Provider;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObjectFactory;
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
  private final Provider<Replicator> replicatorProvider_;
  private final NebuloObjectFactory nebuloObjectFactory_;
  private final String writeFileFormDestination_;

  @Inject
  public ReplicatorResource(Provider<Replicator> replicatorProvider,
      @Named("rest-api.replicator-write-file-form") String writeFileFormDestination,
      NebuloObjectFactory nebuloObjectFactory) {
    replicatorProvider_ = replicatorProvider;
    writeFileFormDestination_ = writeFileFormDestination;
    nebuloObjectFactory_ = nebuloObjectFactory;
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
  @Path("read_file_data")
  @Produces(MediaType.APPLICATION_JSON)
  public String readFileData(@QueryParam("appKey") String key, @QueryParam("objectId") long id)
      throws NebuloException {
    LOGGER.info("Start method readFileData()");
    AppKey appKey = new AppKey(key);
    ObjectId objectId = new ObjectId(BigInteger.valueOf(id));
    JsonPrimitive result;
    try {
      result = new JsonPrimitive(readFile(appKey, objectId));
    } catch (NebuloException e) {
      LOGGER.error(e.getMessage());
      throw e;
    }
    LOGGER.info(result.toString());
    LOGGER.info("End method readFileData()");
    return result.toString();
  }

  @POST
  @Path("write_file_data")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public String writeFileData(
      @FormDataParam("appKey") String key,
      @FormDataParam("objectId") long id,
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail)
      throws NebuloException {
    LOGGER.info("Start method writeFileData()");
    LOGGER.info(fileDetail.getFileName());
    LOGGER.info(JSONFactory.convertFromCollection(fileDetail.getParameters().keySet()).toString());

    AppKey appKey = new AppKey(key);
    ObjectId objectId = new ObjectId(BigInteger.valueOf(id));
    JsonPrimitive result;
    try {
      result = new JsonPrimitive(writeFile(appKey, objectId, uploadedInputStream));
    } catch (NebuloException e) {
      LOGGER.error(e.getMessage());
      throw e;
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    }
    LOGGER.info(result.toString());
    LOGGER.info("End method writeFileData()");
    return result.toString();
  }

  @GET
  @Path("send_file_form")
  @Produces(MediaType.TEXT_HTML)
  public InputStream sendFileForm() throws NebuloException {
    LOGGER.info("Start method sendFileForm()");
    try {
      return new FileInputStream(writeFileFormDestination_);
    } catch (FileNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    } finally {
      LOGGER.info("End method sendFileForm()");
    }
  }

  @GET
  @Path("files_metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public String getFileMeta() {
    LOGGER.info("Start method getFileMeta()");
    JsonElement result = JSONFactory.recursiveConvertFromMap(
            replicatorProvider_.get().getStoredMetaData());
    LOGGER.info(result.toString());
    LOGGER.info("End method getFileMeta()");
    return result.toString();
  }

  @POST
  @Path("delete_file")
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  public String deleteFile(
      @FormParam("appKey") String key,
      @FormParam("objectId") long id)  throws NebuloException {
    LOGGER.info("Start method deleteFile()");
    AppKey appKey = new AppKey(key);
    ObjectId objectId = new ObjectId(BigInteger.valueOf(id));
    NebuloFile file;
    try {
      file = (NebuloFile) nebuloObjectFactory_.fetchExistingNebuloObject(
          new NebuloAddress(appKey, objectId));
      file.delete();
    } catch (NebuloException e) {
      LOGGER.error(e.getMessage());
      throw e;
    }
    LOGGER.info("End method deleteFile()");
    return new JsonPrimitive("OK").toString();
  }

  private String readFile(AppKey appKey, ObjectId objectId) throws NebuloException {
    NebuloFile nebuloFile = (NebuloFile) nebuloObjectFactory_.fetchExistingNebuloObject(
        new NebuloAddress(appKey, objectId));
    StringBuilder stringBuilder = new StringBuilder();
    int currpos = 0;
    int bufSize = 100;
    byte[] data;
    do {
      data = nebuloFile.read(currpos, bufSize);
      currpos += data.length;
      stringBuilder.append(new String(data, StandardCharsets.UTF_8));
    } while (data.length > 0);
    return stringBuilder.toString();
  }

  private int writeFile(AppKey appKey, ObjectId objectId, InputStream uploadedInputStream)
      throws IOException, NebuloException {
    NebuloFile file;
    try {
      file = (NebuloFile) nebuloObjectFactory_.fetchExistingNebuloObject(
          new NebuloAddress(appKey, objectId));
      file.truncate(0);
    } catch (NebuloException e) {
      file = nebuloObjectFactory_.createNewNebuloFile(new NebuloAddress(appKey, objectId));
    }
    return file.write(ByteStreams.toByteArray(uploadedInputStream), 0);
  }
}
