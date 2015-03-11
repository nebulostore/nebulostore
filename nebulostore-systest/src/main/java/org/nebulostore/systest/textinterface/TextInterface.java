package org.nebulostore.systest.textinterface;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import asg.cliche.Command;
import asg.cliche.InputConverter;
import asg.cliche.Param;
import asg.cliche.ShellFactory;
import asg.cliche.ShellManageable;

import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.peers.Peer;

/**
 * A simple text interface to interact with NebuloStore.
 *
 *
 * @author Bolek Kulbabinski
 * @author Krzysztof Rzadca
 */
public final class TextInterface extends Peer implements ShellManageable {
  private NebuloObjectFactory objectFactory_;

  public static final InputConverter[] CLI_INPUT_CONVERTERS = {
    new InputConverter() {
      @Override
      public Object convertInput(String original, Class toClass)
          throws Exception {
        if (toClass.equals(AppKey.class)) {
          return new AppKey(new BigInteger(original));
        } else {
          return null;
        }
      }
    },
    new InputConverter() {
      @Override
      public Object convertInput(String original, Class toClass)
          throws Exception {
        if (toClass.equals(ObjectId.class)) {
          return new ObjectId(new BigInteger(original));
        } else {
          return null;
        }
      }
    },
  };

  @Override
  public void cliEnterLoop() {
    return;
  }

  @Override
  public void cliLeaveLoop() {
    System.out.println("closing nebulostore");
    super.quitNebuloStore();
    System.out.println("closing completed");
  }

  public TextInterface() {
    objectFactory_ = null;
  }

  @Inject
  public void setDependencies(NebuloObjectFactory objectFactory) {
    objectFactory_ = objectFactory;
  }

  @Override
  protected void initializeModules() {
    System.out.print("Starting NebuloStore ...\n");
    super.initializeModules();
  }

  @Override
  protected void runActively() {
    register(appKey_);
    try {
      ShellFactory.createConsoleShell("nebulo", "nebulostore shell.\n" +
          "Type ?list to see available commands.\n" +
          "Type exit to quit the application", this)
          .commandLoop();
    } catch (NullPointerException np) {
      // because of a bug in asg.cliche, empty stdin results in NPE
      System.out.println("Running non-interactively. Kill with a signal");
      info();
      while (true) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        String formattedDate = sdf.format(date);
        System.out.println("[" + formattedDate + "] still alive");
        try {
          Thread.sleep(60000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    } catch (IOException ieo) {
      ieo.printStackTrace();
      System.out.println("Quitting Nebulostore");
      quitNebuloStore();
      System.out.println("Done");
    }
  }

  @Override
  public void quitNebuloStore() {
    System.exit(0);
  }

  @Command(description = "Show nebulo parameters of this instance")
  public void info() {
    System.out.println("appKey: " + appKey_);
    System.out.println("commAddress: " + commAddress_);
  }

  @Command(description = "Download a nebulo file and show it contents")
  public void read(
      @Param(name = "appkey", description = "app key part of the file name") AppKey appKey,
      @Param(name = "objectid", description = "object idpart of the file name") ObjectId objectId) {
    NebuloFile file = getNebuloFile(appKey, objectId);
    if (file == null) {
      return;
    }
    int currpos = 0;
    int bufSize = 100;
    byte[] data;
    do {
      try {
        data = file.read(currpos, bufSize);
        currpos += data.length;
        String str = new String(data, StandardCharsets.UTF_8);
        System.out.print(str);
      } catch (NebuloException exception) {
        System.out.println("Got exception from 'read()' at position " + currpos);
        exception.printStackTrace();
        return;
      }
    } while (data.length > 0);
    System.out.println();
  }


  @Command(description = "Create and write a nebulo file with specified contents")
  public void write(
      @Param(name = "appkey", description = "app key part of the file name") AppKey appKey,
      @Param(name = "objectid", description = "object idpart of the file name") ObjectId objectId,
      @Param(name = "content", description = "content of the file") String... content) {
    NebuloFile file;
    try {
      file = (NebuloFile) objectFactory_.fetchExistingNebuloObject(
          new NebuloAddress(appKey, objectId));
      System.out.println("Successfully fetched existing file");
    } catch (NebuloException e) {
      file = objectFactory_.createNewNebuloFile(new NebuloAddress(appKey, objectId));
      System.out.println("Successfully created new file");
    }
    try {
      int bytesWritten = file.write(
          StringUtils.join(content, " ").getBytes(StandardCharsets.UTF_8), 0);
      System.out.println("Successfully written " + bytesWritten + " bytes");
    } catch (NebuloException exception) {
      exception.printStackTrace();
    }
  }


  @Command(description = "Delete a nebulo file")
  public void delete(
      @Param(name = "appkey", description = "app key part of the file name") AppKey appKey,
      @Param(name = "objectid", description = "object idpart of the file name") ObjectId objectId) {
    NebuloFile file = getNebuloFile(appKey, objectId);
    if (file == null) {
      return;
    }

    try {
      file.delete();
      System.out.println("Successfully deleted file");
    } catch (NebuloException exception) {
      System.out.println("Got exception from 'delete()':\n");
      exception.printStackTrace();
      return;
    }
  }


  protected NebuloFile getNebuloFile(AppKey appKey, ObjectId objectId) {
    try {
      return (NebuloFile) objectFactory_.fetchExistingNebuloObject(new NebuloAddress(appKey,
          objectId));
    } catch (NebuloException exception) {
      System.out.println("Got exception from 'fromAddress()' for " + appKey + " " + objectId);
      exception.printStackTrace();
      return null;
    }
  }
}
