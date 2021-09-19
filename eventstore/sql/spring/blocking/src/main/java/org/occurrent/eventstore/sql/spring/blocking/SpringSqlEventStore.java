package org.occurrent.eventstore.sql.spring.blocking;

import io.cloudevents.CloudEvent;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.blocking.EventStore;
import org.occurrent.eventstore.api.blocking.EventStoreOperations;
import org.occurrent.eventstore.api.blocking.EventStoreQueries;
import org.occurrent.eventstore.api.blocking.EventStream;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class SpringSqlEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

  private final JdbcTemplate jdbcTemplate;
  private final SqlEventStoreConfig sqlEventStoreConfig;

  public SpringSqlEventStore(JdbcTemplate jdbcTemplate, SqlEventStoreConfig sqlEventStoreConfig) {
    this.jdbcTemplate = jdbcTemplate;
    this.sqlEventStoreConfig = sqlEventStoreConfig;

    this.jdbcTemplate.execute(sqlEventStoreConfig.createEventStoreTableSql());
  }

  @Override
  public void write(String streamId, WriteCondition writeCondition, Stream<CloudEvent> events) {
    //jdbcTemplate.execute("create table car (id int, model varchar)");
    //jdbcTemplate.execute("insert into car (id, model) values (1, 'Volkswagen Beetle')");
  }

  @Override
  public void deleteEventStream(String streamId) {

  }

  @Override
  public void deleteEvent(String cloudEventId, URI cloudEventSource) {

  }

  @Override
  public void delete(Filter filter) {

  }

  @Override
  public Optional<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
    return Optional.empty();
  }

  @Override
  public Stream<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
    return null;
  }

  @Override
  public long count(Filter filter) {
    return 0;
  }

  @Override
  public boolean exists(Filter filter) {
    return false;
  }

  @Override
  public boolean exists(String streamId) {
    return false;
  }

  @Override
  public EventStream<CloudEvent> read(String streamId, int skip, int limit) {
    return null;
  }

  @Override
  public void write(String streamId, Stream<CloudEvent> events) {

  }
}
