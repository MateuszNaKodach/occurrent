package org.occurrent.eventstore.sql.spring.reactor;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;

class EventStoreFixture {
  private final DatabaseClient databaseClient;
  private final ReactiveTransactionManager transactionManager;

  EventStoreFixture(DatabaseClient databaseClient, ReactiveTransactionManager transactionManager) {
    this.databaseClient = databaseClient;
    this.transactionManager = transactionManager;
  }

  static EventStoreFixture connectedTo(PostgreSQLContainer<?> postgreSQLContainer) {
    ConnectionFactory connectionFactory = connectionFor(postgreSQLContainer);
    DatabaseClient databaseClient = DatabaseClient.builder()
        .connectionFactory(connectionFactory)
        .namedParameters(true)
        .build();
    ReactiveTransactionManager transactionManager = new R2dbcTransactionManager(connectionFactory);
    return new EventStoreFixture(databaseClient, transactionManager);
  }

  SpringReactorSqlEventStore eventStoreInstance(SqlEventStoreConfig sqlEventStoreConfig) {
    return new SpringReactorSqlEventStore(databaseClient, transactionManager, sqlEventStoreConfig);
  }

  DatabaseClient databaseClient() {
    return databaseClient;
  }

  ReactiveTransactionManager transactionManager() {
    return transactionManager;
  }

  private static PostgresqlConnectionFactory connectionFor(PostgreSQLContainer<?> postgreSQLContainer) {
    return new PostgresqlConnectionFactory(
        PostgresqlConnectionConfiguration.builder()
            .host(postgreSQLContainer.getHost())
            .database(postgreSQLContainer.getDatabaseName())
            .username(postgreSQLContainer.getUsername())
            .password(postgreSQLContainer.getPassword())
            .build()
    );
  }
}

