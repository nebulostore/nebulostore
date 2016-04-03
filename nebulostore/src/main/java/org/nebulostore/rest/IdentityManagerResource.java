package org.nebulostore.rest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.io.ByteStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.identity.IdentityManager;
import org.nebulostore.utils.JSONFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
@Path("identity/")
public class IdentityManagerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdentityManagerResource.class);
  private final String loginForm_;
  private final String registerForm_;
  private final String registerGuestForm_;
  private final String unregisterGuestForm_;
  private IdentityManager identityManager_;

  @Inject
  public IdentityManagerResource(
      @Named("rest-api.html-template.identity-login-form") String loginForm,
      @Named("rest-api.html-template.identity-register-form") String registerForm,
      @Named("rest-api.html-template.identity-register-guest-form") String registerGuestForm,
      @Named("rest-api.html-template.identity-unregister-guest-form") String unregisterGuestForm,
      IdentityManager identityManager) {
    loginForm_ = loginForm;
    registerForm_ = registerForm;
    registerGuestForm_ = registerGuestForm;
    unregisterGuestForm_ = unregisterGuestForm;
    identityManager_ = identityManager;
  }

  @GET
  @Path("register_ep")
  @Produces(MediaType.TEXT_HTML)
  public InputStream register() throws NebuloException {
    LOGGER.info("Start method register()");
    try {
      return new FileInputStream(registerForm_);
    } catch (FileNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    } finally {
      LOGGER.info("End method register()");
    }
  }

  @POST
  @Path("register_update_ep")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public String registerUpdate(
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail)
      throws NebuloException {
    LOGGER.info("Start method registerUpdate()");
    LOGGER.info(fileDetail.getFileName());
    LOGGER.info(JSONFactory.convertFromCollection(fileDetail.getParameters().keySet()).toString());

    JsonPrimitive result;
    try {
      PublicKey publicKey = CryptoUtils.readPublicKey(ByteStreams.toByteArray(uploadedInputStream));
      AppKey appKey = identityManager_.registerUserIdentity(publicKey);
      result = new JsonPrimitive(appKey.toString());
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    }
    LOGGER.info(result.toString());
    LOGGER.info("End method registerUpdate()");
    return result.toString();
  }

  @GET
  @Path("logout_ep")
  @Produces(MediaType.APPLICATION_JSON)
  public String logout() {
    LOGGER.info("Start method logout()");
    identityManager_.logout();
    LOGGER.info("End method logout()");
    return "OK";
  }

  @GET
  @Path("login_ep")
  @Produces(MediaType.TEXT_HTML)
  public InputStream login() throws NebuloException {
    LOGGER.info("Start method login()");
    try {
      return new FileInputStream(loginForm_);
    } catch (FileNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    } finally {
      LOGGER.info("End method login()");
    }
  }

  @POST
  @Path("login_update_ep")
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Produces(MediaType.APPLICATION_JSON)
  public String loginUpdate(
      @FormDataParam("appKey") String key,
      @FormDataParam("type") String type,
      @FormDataParam("file") InputStream uploadedInputStream,
      @FormDataParam("file") FormDataContentDisposition fileDetail)
      throws NebuloException {
    LOGGER.info("Start method loginUpdate()");
    LOGGER.info(key);
    LOGGER.info(type);
    LOGGER.info(fileDetail.getFileName());
    LOGGER.info(JSONFactory.convertFromCollection(fileDetail.getParameters().keySet()).toString());
    try {
      AppKey appKey = new AppKey(key);
      PrivateKey privateKey = CryptoUtils.readPrivateKey(
          ByteStreams.toByteArray(uploadedInputStream));
      if (type.equals("user")) {
        identityManager_.login(appKey, privateKey);
      } else if (type.equals("guest")) {
        identityManager_.loginGuest(appKey, privateKey);
      } else {
        throw new NebuloException("Wrong type");
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    }
    LOGGER.info("End method loginUpdate()");
    return "OK";
  }

  @GET
  @Path("users_list_ep")
  @Produces(MediaType.APPLICATION_JSON)
  public String usersList() {
    LOGGER.info("Start method usersList()");
    JsonElement result = JSONFactory.convertFromCollection(identityManager_.getRegisterUsers());
    LOGGER.info(result.toString());
    LOGGER.info("End method usersList()");
    return result.toString();
  }

  @GET
  @Path("current_user_ep")
  @Produces(MediaType.APPLICATION_JSON)
  public String currentUser() {
    LOGGER.info("Start method currentUser()");
    JsonPrimitive result = new JsonPrimitive(identityManager_.getCurrentUserAppKey().toString());
    LOGGER.info(result.toString());
    LOGGER.info("End method currentUser()");
    return result.toString();
  }

  @GET
  @Path("guests_list_ep")
  @Produces(MediaType.APPLICATION_JSON)
  public String guestsList() {
    LOGGER.info("Start method guestsList()");
    JsonElement result = JSONFactory.convertFromCollection(identityManager_.getRegusterGuests());
    LOGGER.info(result.toString());
    LOGGER.info("End method guestsList()");
    return result.toString();
  }

  @GET
  @Path("register_guest_ep")
  @Produces(MediaType.TEXT_HTML)
  public InputStream registerGuest() throws NebuloException {
    LOGGER.info("Start method registerGuest()");
    try {
      return new FileInputStream(registerGuestForm_);
    } catch (FileNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    } finally {
      LOGGER.info("End method registerGuest()");
    }
  }

  @POST
  @Path("register_guest_update_ep")
  @Produces(MediaType.APPLICATION_JSON)
  public String registerGuestUpdate(
      @FormDataParam("appKey") String key)
      throws NebuloException {
    LOGGER.info("Start method registerGuestUpdate()");
    LOGGER.info(key);
    AppKey appKey = new AppKey(key);
    identityManager_.registerGuestIdentity(appKey);
    JsonPrimitive result = new JsonPrimitive(appKey.toString());
    LOGGER.info(result.toString());
    LOGGER.info("End method registerGuestUpdate()");
    return result.toString();
  }

  @GET
  @Path("unregister_guest_ep")
  @Produces(MediaType.TEXT_HTML)
  public InputStream unregisterGuest() throws NebuloException {
    LOGGER.info("Start method unregisterGuest()");
    try {
      return new FileInputStream(unregisterGuestForm_);
    } catch (FileNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new NebuloException(e);
    } finally {
      LOGGER.info("End method unregisterGuest()");
    }
  }

  @POST
  @Path("unregister_guest_update_ep")
  @Produces(MediaType.APPLICATION_JSON)
  public String unregisterGuestUpdate(
      @FormDataParam("appKey") String key)
      throws NebuloException {
    LOGGER.info("Start method unregisterGuestUpdate()");
    LOGGER.info(key);
    AppKey appKey = new AppKey(key);
    identityManager_.unregisterGuestIdentity(appKey);
    JsonPrimitive result = new JsonPrimitive(appKey.toString());
    LOGGER.info(result.toString());
    LOGGER.info("End method unregisterGuestUpdate()");
    return result.toString();
  }

}
