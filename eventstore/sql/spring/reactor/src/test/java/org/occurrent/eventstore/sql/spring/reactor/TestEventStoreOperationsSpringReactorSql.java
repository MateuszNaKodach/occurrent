package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.eventstore.sql.spring.reactor.CloudEventsDeserializer.deserialize;
import static org.occurrent.eventstore.sql.spring.reactor.Constants.NAME_SOURCE;
import static org.occurrent.filter.Filter.streamId;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.filter.Filter.type;

@Timeout(10)
@Testcontainers
class TestEventStoreOperationsSpringReactorSql implements ReactorEventStoreTestSupport {

  public static final String EVENT_STORE_TABLE_NAME = "occurrent_cloud_events";
  @Container
  private static final PostgreSQLContainer<?> postgreSQLContainer;

  static {
    postgreSQLContainer = Containers.postgreSql();
  }

  @RegisterExtension
  FlushSqlDbExtension flushSqlDbExtension = new FlushSqlDbExtension(
      postgreSQLContainer.getJdbcUrl(),
      postgreSQLContainer.getUsername(),
      postgreSQLContainer.getPassword(),
      EVENT_STORE_TABLE_NAME
  );

  private SpringReactorSqlEventStore eventStore;

  @BeforeEach
  void create_event_store() {
    eventStore = EventStoreFixture
        .connectedTo(postgreSQLContainer)
        .eventStoreInstance(new PostgresSqlEventStoreConfig(EVENT_STORE_TABLE_NAME));
  }

  @Nested
  @DisplayName("delete stream")
  class DeleteStream {

    @Test
    void deleteEventStream_deletes_all_events_in_event_stream() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      StepVerifier.create(eventStore.deleteEventStream(eventStreamId))
          .verifyComplete();

      // Then
      thenEventStream(eventStreamId)
          .hasVersion(0)
          .hasNoEvents();

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(false)
          .verifyComplete();

      StepVerifier.create(eventStore.count(Filter.streamId(eventStreamId)))
          .expectNext(0L)
          .verifyComplete();
    }

  }

  @Nested
  @DisplayName("delete event")
  class DeleteEvent {

    @Test
    void deleteEvent_deletes_only_specific_event_in_event_stream() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      StepVerifier.create(eventStore.deleteEvent(nameWasChanged1.getEventId(), NAME_SOURCE))
          .verifyComplete();

      // Then
      thenEventStream(eventStreamId)
          .hasVersion(1)
          .hasOnlyEvent(nameDefined);

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(true)
          .verifyComplete();

      StepVerifier.create(eventStore.count(Filter.streamId(eventStreamId)))
          .expectNext(1L)
          .verifyComplete();
    }

    @Test
    void delete_deletes_events_according_to_the_filter() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      String anotherEventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      NameDefined nameDefined2 = new NameDefined(UUID.randomUUID().toString(), now, "name2");
      writeEvents(anotherEventStreamId, Collections.singletonList(nameDefined2));

      // When
      eventStore.delete(streamId(eventStreamId).and(time(lte(now.atOffset(UTC).plusMinutes(1))))).block();

      // Then
      thenEventStream(eventStreamId)
          .hasOnlyEvent(nameWasChanged1);

      thenEventStream(anotherEventStreamId)
          .hasOnlyEvent(nameDefined2);
    }
  }

  @Nested
  @DisplayName("update when stream consistency guarantee is transactional")
  class Update {

    @Test
    void updates_cloud_event_and_returns_updated_cloud_event_when_cloud_event_exists() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(anEventId(), now, "name");
      String nameWasChangedEventId = anEventId();
      LocalDateTime nameWasChangedAt = now.plusHours(1);
      NameWasChanged nameWasChanged = new NameWasChanged(nameWasChangedEventId, nameWasChangedAt, "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged));

      // When
      final Mono<CloudEvent> updateNameWasChangedEvent = eventStore.updateEvent(nameWasChangedEventId, NAME_SOURCE, cloudEvent -> {
        NameWasChanged e = deserialize(cloudEvent);
        NameWasChanged correctedName = new NameWasChanged(e.getEventId(), e.getTimestamp(), "name3");
        return CloudEventBuilder.v1(cloudEvent).withData(serializeEvent(correctedName)).build();
      });
      StepVerifier.create(updateNameWasChangedEvent)
          .assertNext(updatedCloudEvent -> {
            NameWasChanged e = deserialize(updatedCloudEvent);
            assertThat(e).isEqualTo(new NameWasChanged(nameWasChangedEventId, now.plusHours(1), "name3"));
          }).verifyComplete();

      // Then
      thenEventStream(eventStreamId)
          .hasOnlyEvents(nameDefined, new NameWasChanged(nameWasChangedEventId, nameWasChangedAt, "name3"));
    }

    @Test
    void returns_empty_mono_when_cloud_event_does_not_exists() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      Mono<CloudEvent> updateCloudEvent = eventStore.updateEvent(anEventId(), NAME_SOURCE, cloudEvent -> {
        NameWasChanged e = deserialize(cloudEvent);
        NameWasChanged correctedName = new NameWasChanged(e.getEventId(), e.getTimestamp(), "name3");
        return CloudEventBuilder.v1(cloudEvent).withData(serializeEvent(correctedName)).build();
      });

      // Then
      StepVerifier.create(updateCloudEvent)
          .verifyComplete();
    }

    @Test
    void throw_iae_when_update_function_returns_null() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      String eventId2 = UUID.randomUUID().toString();
      NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      Mono<CloudEvent> updateCloudEvent = eventStore.updateEvent(eventId2, NAME_SOURCE, cloudEvent -> null);

      // Then
      StepVerifier.create(updateCloudEvent)
          .verifyErrorSatisfies(thrownException ->
              assertThat(thrownException)
                  .isExactlyInstanceOf(IllegalArgumentException.class)
                  .hasMessage("Cloud event update function is not allowed to return null")
          );
    }

    @Test
    void when_update_function_returns_the_same_argument_then_cloud_event_is_unchanged_in_the_database() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      String eventId2 = UUID.randomUUID().toString();
      NameWasChanged nameWasChanged1 = new NameWasChanged(eventId2, now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      // When
      StepVerifier.create(eventStore.updateEvent(eventId2, NAME_SOURCE, Function.identity()))
          .verifyComplete();

      // Then
      thenEventStream(eventStreamId)
          .hasOnlyEvents(nameDefined, nameWasChanged1);
    }

  }


  @Override
  public EventStore eventStore() {
    return this.eventStore;
  }

}
