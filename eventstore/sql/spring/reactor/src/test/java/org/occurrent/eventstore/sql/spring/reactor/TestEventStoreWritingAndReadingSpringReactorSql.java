package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.occurrent.condition.Condition;
import org.occurrent.domain.Composition;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.Name;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.WriteConditionNotFulfilledException;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.occurrent.condition.Condition.and;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.gt;
import static org.occurrent.condition.Condition.gte;
import static org.occurrent.condition.Condition.lt;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.condition.Condition.ne;
import static org.occurrent.condition.Condition.or;
import static org.occurrent.eventstore.sql.spring.reactor.CloudEventsDeserializer.deserialize;

@Timeout(10)
@Testcontainers
class TestEventStoreWritingAndReadingSpringReactorSql implements ReactorEventStoreTestSupport {

  @Container
  private static final PostgreSQLContainer<?> postgreSQLContainer;

  static {
    postgreSQLContainer = Containers.postgreSql();
  }

  private EventStoreFixture eventStoreFixture;
  private EventStore eventStore;

  @BeforeEach
  void create_event_store() {
    eventStoreFixture = EventStoreFixture
        .connectedTo(postgreSQLContainer);
    eventStore = eventStoreFixture
        .eventStoreInstance(new PostgresSqlEventStoreConfig("occurrent_cloud_events"));
  }

  @Test
  void can_read_and_write_single_event() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();
    List<DomainEvent> events = Name.defineName(anEventId(), now, "John Doe");

    //When
    writeEvents(eventStreamId, events, WriteCondition.streamVersionEq(0));

    //Then
    thenEventStream(eventStreamId)
        .hasVersion(1L)
        .hasOnlyEvents(events);
  }

  @Test
  void can_read_and_write_multiple_events_at_once() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();
    List<DomainEvent> events = Composition.chain(Name.defineName(anEventId(), now, "Hello World"), es -> Name.changeName(es, anEventId(), now, "John Doe"));

    //When
    writeEvents(eventStreamId, events, WriteCondition.streamVersionEq(0));

    //Then
    thenEventStream(eventStreamId)
        .hasVersion(2L)
        .hasOnlyEvents(events);
  }

  @Test
  void can_read_and_write_multiple_events_at_different_occasions() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();
    NameDefined nameDefined = new NameDefined(anEventId(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(anEventId(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(anEventId(), now.plusHours(2), "name3");

    //When
    writeEvents(eventStreamId, nameDefined, WriteCondition.streamVersionEq(0));
    writeEvents(eventStreamId, nameWasChanged1, WriteCondition.streamVersionEq(1));
    writeEvents(eventStreamId, nameWasChanged2, WriteCondition.streamVersionEq(2));

    //Then
    thenEventStream(eventStreamId)
        .hasVersion(3L)
        .hasOnlyEvents(nameDefined, nameWasChanged1, nameWasChanged2);
  }


  @Test
  void can_read_events_with_skip_and_limit() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();
    NameDefined nameDefined = new NameDefined(anEventId(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(anEventId(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(anEventId(), now.plusHours(2), "name3");

    //When
    writeEvents(eventStreamId, nameDefined, WriteCondition.streamVersionEq(0));
    writeEvents(eventStreamId, nameWasChanged1, WriteCondition.streamVersionEq(1));
    writeEvents(eventStreamId, nameWasChanged2, WriteCondition.streamVersionEq(2));

    //Then
    thenEventStream(eventStore.read(eventStreamId, 1, 1))
        .hasVersion(3L)
        .hasOnlyEvents(nameWasChanged1);
  }

  @Test
  void stream_version_is_not_updated_when_event_insertion_fails() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();
    List<DomainEvent> events = Composition.chain(Name.defineName(UUID.randomUUID().toString(), now, "Hello World"), es -> Name.changeName(es, UUID.randomUUID().toString(), now, "John Doe"));
    writeEvents(eventStreamId, events, WriteCondition.streamVersionEq(0));

    //When
    StepVerifier.create(persist(eventStreamId, WriteCondition.streamVersionEq(events.size()), events))
        .verifyError(DuplicateCloudEventException.class);

    //Then
    thenEventStream(eventStore.read(eventStreamId))
        .hasVersion(events.size())
        .hasOnlyEvents(events);
  }

  @Test
  void no_events_are_inserted_when_batch_contains_duplicate_events() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();

    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

    //When
    StepVerifier.create(persist(eventStreamId, WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1, nameWasChanged1, nameWasChanged2)))
        .verifyError(DuplicateCloudEventException.class);

    // Then
    thenEventStream(eventStore.read(eventStreamId))
        .notExist();
  }

  @Test
  void no_events_are_inserted_when_batch_contains_event_that_has_already_been_persisted() {
    //Given
    LocalDateTime now = LocalDateTime.now();
    String eventStreamId = anStreamId();

    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name4");

    writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1), WriteCondition.streamVersionEq(0));

    // When
    StepVerifier.create(persist(eventStreamId, WriteCondition.streamVersionEq(2), Flux.just(nameWasChanged2, nameWasChanged1)))
        .verifyError(DuplicateCloudEventException.class);

    // Then
    thenEventStream(eventStore.read(eventStreamId))
        .hasVersion(2)
        .hasOnlyEvents(nameDefined, nameWasChanged1);
  }


  //TODO: Try to refactor with usage of StepVerifier
  @Test
  void read_skew_is_avoided_and_transaction_is_started() {
    // Given
    String eventStreamId = anStreamId();
    LocalDateTime now = LocalDateTime.now();
    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

    persist(eventStreamId, WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1)).block();

    TransactionalOperator transactionalOperator = TransactionalOperator.create(eventStoreFixture.transactionManager());
    CountDownLatch countDownLatch = new CountDownLatch(1);

    AtomicReference<VersionAndEvents> versionAndEventsRef = new AtomicReference<>();

    // When
    transactionalOperator.execute(__ -> eventStore.read(eventStreamId)
        .flatMap(es -> es.events().collectList().map(eventList -> {
          await(countDownLatch);
          return new VersionAndEvents(es.version(), eventList.stream().map(deserialize()).collect(Collectors.toList()));
        }))
        .doOnNext(versionAndEventsRef::set))
        .subscribe();

    transactionalOperator.execute(__ -> persist(eventStreamId, WriteCondition.streamVersionEq(2), nameWasChanged2)
        .then(Mono.fromRunnable(countDownLatch::countDown)).then())
        .blockFirst();

    // Then
    VersionAndEvents versionAndEvents = Awaitility.await().untilAtomic(versionAndEventsRef, not(nullValue()));

    assertAll(
        () -> assertThat(versionAndEvents.version).describedAs("version").isEqualTo(2L),
        () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1)
    );
  }

  //TODO: Try to refactor with usage of StepVerifier
  @Test
  void read_skew_is_avoided_and_skip_and_limit_is_defined_even_when_no_transaction_is_started() {
    // Given
    String eventStreamId = anStreamId();
    LocalDateTime now = LocalDateTime.now();
    NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
    NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
    NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

    persist(eventStreamId, WriteCondition.streamVersionEq(0), Flux.just(nameDefined, nameWasChanged1)).block();

    // When
    VersionAndEvents versionAndEvents =
        eventStore.read(eventStreamId, 0, 2)
            .flatMap(es -> persist(eventStreamId, WriteCondition.streamVersionEq(2), nameWasChanged2)
                .then(es.events().collectList())
                .map(eventList -> new VersionAndEvents(es.version(), eventList.stream().map(deserialize()).collect(Collectors.toList()))))
            .block();
    // Then
    assert versionAndEvents != null;
    assertAll(
        () -> assertThat(versionAndEvents.version).describedAs("version").isEqualTo(2L),
        () -> assertThat(versionAndEvents.events).containsExactly(nameDefined, nameWasChanged1)
    );
  }

  @Nested
  @DisplayName("Conditionally Write to Event Store")
  class ConditionallyWriteToEventStore {

    LocalDateTime now = LocalDateTime.now();

    @Nested
    @DisplayName("eq")
    class Eq {

      @Test
      void writes_events_when_stream_version_matches_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");

        //When
        thenEventStream(eventStreamId)
            .hasVersion(1L);
        writeEvents(eventStreamId, event2, WriteCondition.streamVersionEq(1L));

        //Then
        thenEventStream(eventStreamId)
            .hasVersion(2L)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_does_not_match_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersionEq(10), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown ->
                assertThat(thrown)
                    .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                    .hasMessage("WriteCondition was not fulfilled. Expected version to be equal to 10 but was 1.")
            );
      }


    }

    @Nested
    @DisplayName("ne")
    class Ne {

      @Test
      void writes_events_when_stream_version_does_not_match_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(anEventId(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        //When
        DomainEvent event2 = new NameWasChanged(anEventId(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(ne(20L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_match_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(anEventId(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(anEventId(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(ne(1L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown ->
                assertThat(thrown).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                    .hasMessage("WriteCondition was not fulfilled. Expected version to not be equal to 1 but was 1.")
            );
      }

    }

    @Nested
    @DisplayName("lt")
    class Lt {

      @Test
      void writes_events_when_stream_version_is_less_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(lt(10L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_is_greater_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(lt(0L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown ->
                assertThat(thrown).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                    .hasMessage("WriteCondition was not fulfilled. Expected version to be less than 0 but was 1.")
            );
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_is_equal_to_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(lt(1L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown -> assertThat(thrown)
                .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                .hasMessage("WriteCondition was not fulfilled. Expected version to be less than 1 but was 1.")
            );
      }
    }


    @Nested
    @DisplayName("gt")
    class Gt {

      @Test
      void writes_events_when_stream_version_is_greater_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(gt(0L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_is_less_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(gt(100L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown ->
                assertThat(thrown)
                    .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                    .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than 100 but was 1.")
            );

      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_is_equal_to_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(gt(1L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown ->
                assertThat(thrown).isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                    .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than 1 but was 1.")
            );
      }
    }

    @Nested
    @DisplayName("lte")
    class Lte {

      @Test
      void writes_events_when_stream_version_is_less_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(lte(10L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }


      @Test
      void writes_events_when_stream_version_is_equal_to_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(lte(1L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_is_greater_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(lte(0L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown -> assertThat(thrown)
                .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                .hasMessage("WriteCondition was not fulfilled. Expected version to be less than or equal to 0 but was 1.")
            );
      }
    }

    @Nested
    @DisplayName("gte")
    class Gte {

      @Test
      void writes_events_when_stream_version_is_greater_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(gte(0L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void writes_events_when_stream_version_is_equal_to_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(gte(0L)));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_stream_version_is_less_than_expected_version() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(gte(100L)), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown -> assertThat(thrown)
                .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 100 but was 1.")
            );
      }
    }

    @Nested
    @DisplayName("and")
    class And {

      @Test
      void writes_events_when_stream_version_is_when_all_conditions_match_and_expression() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        //When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(and(gte(0L), lt(100L), ne(40L))));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_any_of_the_operations_in_the_and_expression_is_not_fulfilled() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(and(gte(0L), lt(100L), ne(1L))), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown -> assertThat(thrown)
                .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 0 and to be less than 100 and to not be equal to 1 but was 1.")
            );
      }
    }

    @Nested
    @DisplayName("or")
    class Or {

      @Test
      void writes_events_when_stream_version_is_when_any_condition_in_or_expression_matches() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        //When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(or(gte(100L), lt(0L), ne(40L))));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_none_of_the_operations_in_the_and_expression_is_fulfilled() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(or(gte(100L), lt(1L))), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown -> assertThat(thrown)
                .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                .hasMessage("WriteCondition was not fulfilled. Expected version to be greater than or equal to 100 or to be less than 1 but was 1.")
            );
      }
    }

    @Nested
    @DisplayName("not")
    class Not {

      @Test
      void writes_events_when_stream_version_is_not_matching_condition() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        //When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        writeEvents(eventStreamId, event2, WriteCondition.streamVersion(Condition.not(eq(100L))));

        // Then
        thenEventStream(eventStreamId)
            .hasOnlyEvents(event1, event2);
      }

      @Test
      void throws_write_condition_not_fulfilled_when_condition_is_fulfilled_but_should_not_be_so() {
        // Given
        String eventStreamId = anStreamId();
        DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
        writeEvents(eventStreamId, event1);

        // When
        DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
        Mono<Void> writeEvents = persist(eventStreamId, WriteCondition.streamVersion(Condition.not(eq(1L))), event2);

        // Then
        StepVerifier.create(writeEvents)
            .verifyErrorSatisfies(thrown -> assertThat(thrown)
                .isExactlyInstanceOf(WriteConditionNotFulfilledException.class)
                .hasMessage("WriteCondition was not fulfilled. Expected version not to be equal to 1 but was 1.")
            );
      }
    }

  }

  @Override
  public EventStore eventStore() {
    return this.eventStore;
  }
}

/*
abstract class AbstractR2dbcConfiguration {

  public DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
    return DatabaseClient.builder()
        .connectionFactory(connectionFactory)
        .bindMarkers(DialectResolver.getDialect(connectionFactory).getBindMarkersFactory())
        .build();
  }

  public ConnectionPool connectionFactory() {
    ConnectionFactory connectionFactory = createConnectionFactory(toSpringDataAutoConfigurationProperties(r2dbcProperties()));
    R2dbcProperties.Pool pool = r2dbcProperties().getPool();
    ConnectionPoolConfiguration.Builder builder = ConnectionPoolConfiguration.builder(connectionFactory)
        .name(pool.getName())
        .maxSize(pool.getMaxSize())
        .initialSize(pool.getInitialSize())
        .maxIdleTime(pool.getMaxIdleTime())
        .maxLifeTime(pool.getMaxLifeTime())
        .maxAcquireTime(pool.getMaxAcquireTime())
        .maxCreateConnectionTime(pool.getMaxCreateConnectionTime());
    if (StringUtils.hasText(pool.getValidationQuery())) {
      builder.validationQuery(pool.getValidationQuery());
    }
    return new ConnectionPool(builder.build());
  }

  abstract R2dbcProperties r2dbcProperties();

  ReactiveTransactionManager reactiveTransactionManager(ConnectionFactory connectionFactory) {
    return new R2dbcTransactionManager(connectionFactory);
  }

  private static ConnectionFactory createConnectionFactory(R2dbcProperties properties) {
    return ConnectionFactoryBuilder.of(properties, () -> EmbeddedDatabaseConnection.NONE).build();
  }

  private static R2dbcProperties toSpringDataAutoConfigurationProperties(R2dbcProperties R2dbcProperties) {
    var prop = new R2dbcProperties();
    prop.setName(R2dbcProperties.getName());
    prop.setGenerateUniqueName(R2dbcProperties.isGenerateUniqueName());
    prop.setUrl(R2dbcProperties.getUrl());
    prop.setUsername(R2dbcProperties.getUsername());
    prop.setPassword(R2dbcProperties.getPassword());
    prop.getProperties().putAll(R2dbcProperties.getProperties());
    var poolProp = prop.getPool();
    var PoolProp = R2dbcProperties.getPool();
    poolProp.setInitialSize(PoolProp.getInitialSize());
    poolProp.setMaxIdleTime(PoolProp.getMaxIdleTime());
    poolProp.setMaxSize(PoolProp.getMaxSize());
    poolProp.setValidationQuery(PoolProp.getValidationQuery());
    return prop;
  }
}
 */
