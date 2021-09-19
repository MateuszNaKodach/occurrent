package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Row;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.cloudevents.OccurrentExtensionGetter;
import org.occurrent.eventstore.api.LongConditionEvaluator;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.WriteResult;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreOperations;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.SqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.occurrent.eventstore.api.WriteCondition.anyStreamVersion;

//TODO: Consider Spring framework agnostic - just R2DBC
//https://hantsy.medium.com/introduction-to-r2dbc-82058417644b
//https://medium.com/swlh/working-with-relational-database-using-r2dbc-databaseclient-d61a60ebc67f
//https://hantsy.medium.com/
//https://stackoverflow.com/questions/57971278/connection-pool-size-with-postgres-r2dbc-pool
class SpringReactorSqlEventStore implements EventStore, EventStoreOperations, EventStoreQueries {

  private final DatabaseClient databaseClient;
  private final TransactionalOperator transactionalOperator;
  private final SqlEventStoreConfig sqlEventStoreConfig;

  SpringReactorSqlEventStore(DatabaseClient databaseClient, ReactiveTransactionManager reactiveTransactionManager, SqlEventStoreConfig sqlEventStoreConfig) {
    requireNonNull(databaseClient, DatabaseClient.class.getSimpleName() + " cannot be null");
    requireNonNull(reactiveTransactionManager, ReactiveTransactionManager.class.getSimpleName() + " cannot be null");
    requireNonNull(sqlEventStoreConfig, SqlEventStoreConfig.class.getSimpleName() + " cannot be null");

    this.databaseClient = databaseClient;
    this.sqlEventStoreConfig = sqlEventStoreConfig;
    this.transactionalOperator = TransactionalOperator.create(reactiveTransactionManager);
    initializeEventStore(databaseClient, sqlEventStoreConfig).block(); //TODO: Consider move invocation away from constructor
  }

  static Mono<Void> initializeEventStore(DatabaseClient databaseClient, SqlEventStoreConfig sqlEventStoreConfig) {
    return databaseClient.sql(sqlEventStoreConfig.createEventStoreTableSql()).then();
  }

  @Override
  public Mono<WriteResult> write(String streamId, WriteCondition writeCondition, Flux<CloudEvent> events) {
    final Flux<Void> eventsToWrite = currentStreamVersionFulfillsCondition(streamId, writeCondition)
        .flatMapMany(SpringReactorSqlEventStore::streamVersionIncrements)
        .zipWith(events)
        .flatMap(insertCloudEventTo(streamId));
    return eventsToWrite.as(transactionalOperator::transactional)
        .map()
        .onErrorMap(DataIntegrityViolationException.class, DataIntegrityViolationToDuplicateCloudEventExceptionTranslator::translateToDuplicateCloudEventException);
  }

  private Function<Tuple2<Long, CloudEvent>, Mono<Void>> insertCloudEventTo(String streamId) {
    String insertEventSql = "INSERT INTO " + sqlEventStoreConfig.eventStoreTableName() + " "
        + "(id, source, specversion, type, datacontenttype, dataschema, subject, streamid, streamversion, data, time) VALUES"
        + "(:id, :source, :specversion, :type, :datacontenttype, :dataschema, :subject, :streamid, :streamversion, :data, :time)";
    return streamVersionAndEvent -> {
      long streamVersion = streamVersionAndEvent.getT1();
      CloudEvent event = streamVersionAndEvent.getT2();
      return databaseClient.sql(insertEventSql)
          .bind("id", event.getId())
          .bind("source", event.getSource())
          .bind("specversion", event.getSpecVersion().toString())
          .bind("type", event.getType())
          .bind("datacontenttype", event.getDataContentType())
          .bind("dataschema", Parameter.fromOrEmpty(event.getDataSchema(), URI.class))
          .bind("subject", event.getSubject())
          .bind(OccurrentCloudEventExtension.STREAM_ID, Parameter.fromOrEmpty(streamId, String.class))
          .bind(OccurrentCloudEventExtension.STREAM_VERSION, Parameter.fromOrEmpty(streamVersion, String.class))
          .bind("data", Parameter.fromOrEmpty(event.getData() != null ? event.getData().toBytes() : null, Object.class))
          .bind("time", event.getTime())
          .then();
    };
  }

  //TODO: Remove Duplication - same in ReactorMongoEventStore
  private Mono<Long> currentStreamVersionFulfillsCondition(String streamId, WriteCondition writeCondition) {
    return currentStreamVersion(streamId)
        .flatMap(currentStreamVersion -> isFulfilled(currentStreamVersion, writeCondition)
            ? Mono.just(currentStreamVersion)
            : Mono.error(new WriteConditionNotFulfilledException(streamId, currentStreamVersion, writeCondition, String.format("%s was not fulfilled. Expected version %s but was %s.", WriteCondition.class.getSimpleName(), writeCondition.toString(), currentStreamVersion)))
        );
  }

  //TODO: Remove Duplication - same in ReactorMongoEventStore
  //FIXME: Change to something more extendable - Specification pattern
  private static boolean isFulfilled(long streamVersion, WriteCondition writeCondition) {
    if (writeCondition.isAnyStreamVersion()) {
      return true;
    }

    if (!(writeCondition instanceof WriteCondition.StreamVersionWriteCondition)) {
      throw new IllegalArgumentException("Invalid " + WriteCondition.class.getSimpleName() + ": " + writeCondition);
    }

    WriteCondition.StreamVersionWriteCondition c = (WriteCondition.StreamVersionWriteCondition) writeCondition;
    return LongConditionEvaluator.evaluate(c.condition, streamVersion);
  }

  //TODO: Remove Duplication - same in ReactorMongoEventStore
  private static Flux<Long> streamVersionIncrements(Long currentStreamVersion) {
    return Flux.generate(() -> currentStreamVersion, (version, sink) -> {
      long nextVersion = version + 1L;
      sink.next(nextVersion);
      return nextVersion;
    });
  }

  private Mono<Long> currentStreamVersion(String streamId) {
    String sql = "SELECT COALESCE(MAX(streamversion),0) FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamid = :streamid";

    return databaseClient.sql(sql)
        .bind("streamid", streamId)
        .map(result -> result.get(0, Long.class))
        .one()
        .switchIfEmpty(Mono.just(0L));
  }

  @Override
  public Mono<Void> deleteEventStream(String streamId) {
    String sql = "DELETE FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamid = :streamid";
    return databaseClient.sql(sql).bind("streamid", streamId).then();
  }

  @Override
  public Mono<Void> deleteEvent(String cloudEventId, URI cloudEventSource) {
    String sql = "DELETE FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE id = :id AND source = :source";
    return databaseClient.sql(sql)
        .bind("id", cloudEventId)
        .bind("source", cloudEventSource.toString())
        .then();
  }

  @Override
  public Mono<Void> delete(Filter filter) {
    String sql = "DELETE FROM " + sqlEventStoreConfig.eventStoreTableName() + FilterConverter.convertFilterToWhereClause(filter);
    return databaseClient.sql(sql).then();
  }

  @Override
  public Mono<CloudEvent> updateEvent(String cloudEventId, URI cloudEventSource, Function<CloudEvent, CloudEvent> updateFunction) {
    String selectCurrentEventSql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE id = :id AND source = :source";
    String updateSql = "UPDATE " + sqlEventStoreConfig.eventStoreTableName() + " SET specversion = :specversion, type = :type, datacontenttype = :datacontenttype, dataschema = :dataschema, subject = :subject, streamid = :streamid, streamversion = :streamversion, data = :data, time = :time WHERE id = :id AND source = :source";
    final Mono<CloudEvent> selectCurrentCloudEvent = databaseClient.sql(selectCurrentEventSql)
        .bind("id", cloudEventId)
        .bind("source", cloudEventSource.toString())
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .one()
        .flatMap(cloudEventMono -> cloudEventMono);
    final Mono<CloudEvent> updateCloudEvent = selectCurrentCloudEvent
        .flatMap(currentCloudEvent -> {
          CloudEvent updatedCloudEvent = updateFunction.apply(currentCloudEvent);
          if (updatedCloudEvent == null) {
            return Mono.error(new IllegalArgumentException("Cloud event update function is not allowed to return null"));
          }
          if (Objects.equals(updatedCloudEvent, currentCloudEvent)) {
            return Mono.empty();
          }
          String streamId = OccurrentExtensionGetter.getStreamId(currentCloudEvent);
          long streamVersion = OccurrentExtensionGetter.getStreamVersion(currentCloudEvent);
          return databaseClient.sql(updateSql)
              .bind("specversion", updatedCloudEvent.getSpecVersion().toString())
              .bind("type", updatedCloudEvent.getType())
              .bind("datacontenttype", updatedCloudEvent.getDataContentType())
              .bind("dataschema", Parameter.fromOrEmpty(updatedCloudEvent.getDataSchema(), URI.class))
              .bind("subject", updatedCloudEvent.getSubject())
              .bind(OccurrentCloudEventExtension.STREAM_ID, Parameter.fromOrEmpty(streamId, String.class))
              .bind(OccurrentCloudEventExtension.STREAM_VERSION, Parameter.fromOrEmpty(streamVersion, String.class))
              .bind("data", Parameter.fromOrEmpty(updatedCloudEvent.getData() != null ? updatedCloudEvent.getData().toBytes() : null, Object.class))
              .bind("time", updatedCloudEvent.getTime())
              .bind("id", cloudEventId)
              .bind("source", cloudEventSource.toString())
              .then().thenReturn(updatedCloudEvent);
        });
    return updateCloudEvent.as(transactionalOperator::transactional);
  }

  @Override
  public Flux<CloudEvent> query(Filter filter, int skip, int limit, SortBy sortBy) {
    String sql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName() + FilterConverter.convertFilterToWhereClause(filter) + toOrderBySql(sortBy) + " LIMIT :limit OFFSET :offset";
    return databaseClient.sql(sql)
        .bind("limit", limit)
        .bind("offset", skip)
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .all()
        .flatMap(cloudEventMono -> cloudEventMono);
  }

  private String toOrderBySql(SortBy sortBy) {
    if (sortBy == SortBy.NATURAL_ASC || sortBy == SortBy.NATURAL_DESC) {
      return "";
    }
    if (sortBy == SortBy.TIME_ASC) {
      return " ORDER BY time ASC ";
    }
    if (sortBy == SortBy.TIME_DESC) {
      return " ORDER BY time DESC ";
    }
    return "";
  }

  @Override
  public Mono<Long> count(Filter filter) {
    String sql = "SELECT COUNT(*) FROM " + sqlEventStoreConfig.eventStoreTableName() + FilterConverter.convertFilterToWhereClause(filter);
    return databaseClient.sql(sql).map(row -> row.get(0, Long.class)).one();
  }

  @Override
  public Mono<Boolean> exists(Filter filter) {
    String sql = "SELECT EXISTS(SELECT 1 FROM " + sqlEventStoreConfig.eventStoreTableName() + FilterConverter.convertFilterToWhereClause(filter) + ")";
    return databaseClient.sql(sql).map(row -> row.get(0, Boolean.class)).one();
  }

  @Override
  public Mono<Boolean> exists(String streamId) {
    return exists(Filter.streamId(streamId));
  }

  //https://stackoverflow.com/questions/57942240/how-to-extract-jsonb-from-postgresql-to-spring-webflux-using-r2dbc
  //https://github.com/r2dbc/r2dbc-h2/issues/115
  @Override
  public Mono<EventStream<CloudEvent>> read(String streamId, int skip, int limit) {
    String sql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName() + " WHERE streamid = :streamid ORDER BY streamversion ASC LIMIT :limit OFFSET :offset";
    final Mono<Long> currentStreamVersion = currentStreamVersion(streamId);
    final Flux<CloudEvent> cloudEvents = databaseClient.sql(sql)
        .bind("streamid", streamId)
        .bind("limit", limit)
        .bind("offset", skip)
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .all()
        .flatMap(cloudEventMono -> cloudEventMono);
    return transactionalOperator.execute(__ ->
        currentStreamVersion
            .flatMap(streamVersion -> Mono.just(new EventStreamEntity(streamId, streamVersion, cloudEvents)))
            .defaultIfEmpty(new EventStreamEntity(streamId, 0L, Flux.empty()))
    ).single().map(cloudEvent -> cloudEvent);
  }

  @Override
  public Flux<CloudEvent> all() {
    String sql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName();
    return databaseClient.sql(sql)
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .all()
        .flatMap(cloudEventMono -> cloudEventMono);
  }

  @Override //TODO: How to implement data? Bloba is not good for querying?
  public Flux<CloudEvent> query(Filter filter) {
    String sql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName() + FilterConverter.convertFilterToWhereClause(filter);
    return databaseClient.sql(sql)
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .all()
        .flatMap(cloudEventMono -> cloudEventMono);
  }

  @Override
  public Flux<CloudEvent> all(int skip, int limit) {
    String sql = "SELECT * FROM " + sqlEventStoreConfig.eventStoreTableName() + " LIMIT :limit OFFSET :offset";
    return databaseClient.sql(sql)
        .bind("limit", limit)
        .bind("offset", skip)
        .map(SpringReactorSqlEventStore::eventStreamRowMapper)
        .all()
        .flatMap(cloudEventMono -> cloudEventMono);
  }

  private static Mono<CloudEvent> eventStreamRowMapper(Row row) {
    final String id = row.get("id", String.class);
    final String source = row.get("source", String.class);
    final String specVersion = requireNonNull(row.get("specversion", String.class));
    final String type = row.get("type", String.class);
    final String dataContentType = row.get("datacontenttype", String.class);
    final String dataSchema = row.get("dataschema", String.class);
    final String subject = row.get("subject", String.class);
    final String streamId = row.get(OccurrentCloudEventExtension.STREAM_ID, String.class);
    final Long streamVersion = row.get(OccurrentCloudEventExtension.STREAM_VERSION, Long.class);
    final Blob data = row.get("data", Blob.class);
    final OffsetDateTime time = row.get("time", OffsetDateTime.class);
    CloudEventBuilder cloudEvent = CloudEventBuilder.fromSpecVersion(SpecVersion.parse(specVersion))
        .withId(id)
        .withSource(source != null ? URI.create(source) : null)
        .withType(type)
        .withDataContentType(dataContentType)
        .withDataSchema(dataSchema != null ? URI.create(dataSchema) : null)
        .withTime(time)
        .withSubject(subject);
    if (streamId != null) {
      cloudEvent = cloudEvent.withExtension(OccurrentCloudEventExtension.STREAM_ID, streamId);
    }
    if (streamVersion != null) {
      cloudEvent = cloudEvent.withExtension(OccurrentCloudEventExtension.STREAM_VERSION, streamVersion);
    }
    final Mono<byte[]> eventData = data == null
        ? Mono.empty()
        : Mono.from(data.stream())
        .map(SpringReactorSqlEventStore::toByteArray);
    final CloudEventBuilder finalCloudEvent = cloudEvent;
    return eventData
        .map(bytes -> finalCloudEvent.withData(bytes).build())
        .defaultIfEmpty(finalCloudEvent.build());
  }

  private static byte[] toByteArray(ByteBuffer byteBuffer) {
    byte[] byteArray = new byte[byteBuffer.remaining()];
    byteBuffer.get(byteArray, 0, byteArray.length);
    return byteArray;
  }

  @Override
  public Mono<Void> write(String streamId, Flux<CloudEvent> events) {
    return write(streamId, anyStreamVersion(), events);
  }

  private static class EventStreamEntity implements EventStream<CloudEvent> {
    private final String id;
    private final long version;
    private final Flux<CloudEvent> events;

    EventStreamEntity(String id, long version, Flux<CloudEvent> events) {
      this.id = id;
      this.version = version;
      this.events = events;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public Flux<CloudEvent> events() {
      return events;
    }
  }
}
