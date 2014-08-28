package org.nebulostore.persistence;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.base.Function;

public class SQLKeyValueStore<T> implements KeyValueStore<T> {

  private final Function<T, byte[]> serializer_;
  private final Function<byte[], T> deserializer_;
  private Connection connection_;
  private boolean canUpdateKeys_;

  public SQLKeyValueStore(String host, String port, String database, String user, String password,
      boolean canUpdateKeys, Function<T, byte[]> serializer, Function<byte[], T> deserializer)
      throws IOException {
    serializer_ = serializer;
    deserializer_ = deserializer;
    canUpdateKeys_ = canUpdateKeys;
    try {
      connection_ = DriverManager
          .getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database,
              user, password);
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void put(String key, T value) throws IOException {
    T oldVal = get(key);
    if (oldVal != null && canUpdateKeys_) {
      update(key, value);
    } else if (oldVal != null) {
      throw new IOException("Object with key " + key + " already exists in the database!");
    } else {
      String sql = "INSERT INTO \"KeyValueStore\" (key_str, value) VALUES (?, ?)";
      try {
        PreparedStatement preparedStatement = connection_.prepareStatement(sql);
        try {
          preparedStatement.setString(1, key);
          preparedStatement.setBytes(2, serializer_.apply(value));
          preparedStatement.execute();
        } finally {
          preparedStatement.close();
        }
      } catch (SQLException e) {
        throw new IOException(e.getMessage());
      }
    }
  }

  public T executeSelectKey(String key) throws IOException {
    String sql = "SELECT value FROM \"KeyValueStore\" WHERE key_str = ? " +
        "ORDER BY id DESC LIMIT 1 FOR UPDATE";
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement(sql);
      try {
        preparedStatement.setString(1, key);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
          return deserializer_.apply(resultSet.getBytes(1));
        }
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
    throw new IOException("Element " + key + " does not exits!");
  }

  @Override
  public T get(String key) {
    try {
      return executeSelectKey(key);
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public void delete(String key) throws IOException {
    String sql = "DELETE FROM \"KeyValueStore\" WHERE key_str = ?";
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement(sql);
      try {
        preparedStatement.setString(1, key);
        preparedStatement.execute();
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void startTransaction() throws IOException {
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement("BEGIN");
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void commitTransaction() throws IOException {
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement("COMMIT");
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void rollbackTransaction() throws IOException {
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement("ROLLBACK");
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void update(String key, T value) throws IOException {
    String sql = "UPDATE \"KeyValueStore\" SET value = ? WHERE key_str = ?";
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement(sql);
      try {
        preparedStatement.setBytes(1, serializer_.apply(value));
        preparedStatement.setString(2, key);
        preparedStatement.execute();
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void performTransaction(String key, Function<T, T> function)
      throws IOException {
    startTransaction();
    try {
      T oldVal;
      try {
        oldVal = executeSelectKey(key);
      } catch (IOException e) {
        oldVal = null;
      }
      T newVal = function.apply(oldVal);
      update(key, newVal);
      commitTransaction();
    } catch (IOException e) {
      rollbackTransaction();
      throw e;
    }
  }

  protected void finalize() throws Throwable {
    if (connection_ != null) {
      connection_.close();
    }
    super.finalize();
  }

}
