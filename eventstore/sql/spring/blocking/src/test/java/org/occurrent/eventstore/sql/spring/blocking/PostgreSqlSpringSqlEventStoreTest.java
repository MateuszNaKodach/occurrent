package org.occurrent.eventstore.sql.spring.blocking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.util.stream.Stream;

@Timeout(10)
@Testcontainers
class PostgreSqlSpringSqlEventStoreTest {

  @Container
  private static final PostgreSQLContainer<?> postgreSQLContainer;

  static {
    postgreSQLContainer = new PostgreSQLContainer<>("postgres:13-alpine")
        .withDatabaseName("occurrent")
        .withExposedPorts(5432);
  }

  private static final URI NAME_SOURCE = URI.create("http://name");
  private ObjectMapper objectMapper;

  private EventStore eventStore;

  @BeforeEach
  void create_event_store() {
    SingleConnectionDataSource dataSource = dataSourceFrom(postgreSQLContainer);
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    SqlEventStoreConfig sqlEventStoreConfig = new PostgresSqlEventStoreConfig("occurrent_cloud_events");
    eventStore = new SpringSqlEventStore(jdbcTemplate, sqlEventStoreConfig);
  }

  @Test
  void name() {
    eventStore.write("test", Stream.empty());
  }

  private SingleConnectionDataSource dataSourceFrom(PostgreSQLContainer<?> postgreSQLContainer) {
    SingleConnectionDataSource dataSource = new SingleConnectionDataSource();
    dataSource.setDriverClassName(postgreSQLContainer.getDriverClassName());
    dataSource.setUrl(postgreSQLContainer.getJdbcUrl());
    dataSource.setUsername(postgreSQLContainer.getUsername());
    dataSource.setPassword(postgreSQLContainer.getPassword());
    return dataSource;
  }
}
