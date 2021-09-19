package org.occurrent.eventstore.sql.spring.reactor;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.Connection;
import java.sql.DriverManager;


class FlushSqlDbExtension implements BeforeEachCallback {

  private final String jdbcUrl;
  private final String user;
  private final String password;
  private final String tableName;

  FlushSqlDbExtension(String jdbcUrl, String user, String password, String tableName) {
    this.jdbcUrl = jdbcUrl;
    this.user = user;
    this.password = password;
    this.tableName = tableName;
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    try {
      flushDb(0);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void flushDb(int attempt) throws InterruptedException {
    try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
      connection.prepareStatement("DROP TABLE IF EXISTS " + tableName).execute();
    } catch (Throwable t) {
      System.out.println("Failed to flush database:" + t);
      Thread.sleep(100);
      if (attempt < 10) {
        flushDb(attempt + 1);
      }
    }
  }
}
