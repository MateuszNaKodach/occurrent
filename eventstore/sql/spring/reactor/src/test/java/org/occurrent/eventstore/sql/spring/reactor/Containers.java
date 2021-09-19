package org.occurrent.eventstore.sql.spring.reactor;

import org.testcontainers.containers.PostgreSQLContainer;

import java.util.ArrayList;
import java.util.List;

class Containers {

  static PostgreSQLContainer<?> postgreSql() {
    final PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:13-alpine")
        .withDatabaseName("occurrent")
        .withUsername("occurrent-user")
        .withPassword("occurrent-password")
        .withExposedPorts(5432);
    List<String> ports = new ArrayList<>();
    ports.add("5432:5432");
    postgreSQLContainer.setPortBindings(ports);
    return postgreSQLContainer;
  }
}
