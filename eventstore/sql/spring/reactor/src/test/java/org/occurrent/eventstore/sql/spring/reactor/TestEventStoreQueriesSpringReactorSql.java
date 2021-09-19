package org.occurrent.eventstore.sql.spring.reactor;

import com.mongodb.ConnectionString;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.occurrent.cloudevents.OccurrentCloudEventExtension;
import org.occurrent.domain.DomainEvent;
import org.occurrent.domain.NameDefined;
import org.occurrent.domain.NameWasChanged;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStoreQueries;
import org.occurrent.eventstore.sql.common.PostgresSqlEventStoreConfig;
import org.occurrent.filter.Filter;
import org.occurrent.testsupport.mongodb.FlushMongoDBExtension;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.occurrent.condition.Condition.and;
import static org.occurrent.condition.Condition.eq;
import static org.occurrent.condition.Condition.gt;
import static org.occurrent.condition.Condition.gte;
import static org.occurrent.condition.Condition.lt;
import static org.occurrent.condition.Condition.lte;
import static org.occurrent.eventstore.sql.spring.reactor.Constants.NAME_SOURCE;
import static org.occurrent.eventstore.sql.spring.reactor.EventQueryAssertions.thenEventQuery;
import static org.occurrent.filter.Filter.cloudEvent;
import static org.occurrent.filter.Filter.data;
import static org.occurrent.filter.Filter.dataSchema;
import static org.occurrent.filter.Filter.id;
import static org.occurrent.filter.Filter.source;
import static org.occurrent.filter.Filter.streamId;
import static org.occurrent.filter.Filter.subject;
import static org.occurrent.filter.Filter.time;
import static org.occurrent.filter.Filter.type;

@Timeout(10)
@Testcontainers
class TestEventStoreQueriesSpringReactorSql implements ReactorEventStoreTestSupport {

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
  @DisplayName("count events")
  class CountTest {

    @Test
    void count_without_any_filter_returns_all_the_count_of_all_events_in_the_event_store() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      // When
      final Mono<Long> allEventsCount = eventStore.count();

      // Then
      StepVerifier.create(allEventsCount)
          .expectNext(3L)
          .verifyComplete();
    }

    @Test
    void count_with_filter_returns_only_events_that_matches_the_filter() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      // When
      final Mono<Long> filteredEventsCount = eventStore.count(type(NameDefined.class.getName()));

      // Then
      StepVerifier.create(filteredEventsCount)
          .expectNext(2L)
          .verifyComplete();
    }
  }

  @Nested
  @DisplayName("event exists")
  class EventExists {

    @Test
    void returns_false_when_there_are_no_events_in_the_event_store_and_filter_is_all() {
      StepVerifier.create(eventStore.exists(Filter.all()))
          .expectNext(false)
          .verifyComplete();
    }

    @Test
    void returns_true_when_there_are_events_in_the_event_store_and_filter_is_all() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      StepVerifier.create(eventStore.exists(Filter.all()))
          .expectNext(true)
          .verifyComplete();
    }

    @Test
    void returns_false_when_there_are_no_events_in_the_event_store_and_filter_is_not_all() {
      StepVerifier.create(eventStore.exists(type(NameDefined.class.getName())))
          .expectNext(false)
          .verifyComplete();
    }

    @Test
    void returns_true_when_there_are_matching_events_in_the_event_store_and_filter_not_all() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameWasChanged(UUID.randomUUID().toString(), now, "Jan Doe");
      DomainEvent event3 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2, event3));

      StepVerifier.create(eventStore.exists(type(NameDefined.class.getName())))
          .expectNext(true)
          .verifyComplete();
    }

    @Test
    void returns_false_when_there_events_in_the_event_store_that_doesnt_match_filter() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      DomainEvent event1 = new NameDefined(UUID.randomUUID().toString(), now, "John Doe");
      DomainEvent event2 = new NameDefined(UUID.randomUUID().toString(), now, "Hello Doe");
      writeEvents(eventStreamId, Arrays.asList(event1, event2));

      StepVerifier.create(eventStore.exists(type(NameWasChanged.class.getName())))
          .expectNext(false)
          .verifyComplete();
    }
  }

  @Nested
  @DisplayName("stream exists")
  class StreamExists {

    @Test
    void returns_true_when_stream_exists_and_contains_events() {
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      writeEvents(eventStreamId, Arrays.asList(nameDefined, nameWasChanged1));

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(true)
          .verifyComplete();
    }

    @Test
    void returns_false_when_no_events_have_been_persisted_to_stream() {
      String eventStreamId = anStreamId();

      StepVerifier.create(eventStore.exists(eventStreamId))
          .expectNext(false)
          .verifyComplete();
    }
  }

  //TODO: QueriesTest
  @Nested
  @DisplayName("queries")
  class QueriesTest {

    @Test
    void all_without_skip_and_limit_returns_all_events() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String eventStreamId3 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, nameDefined);
      writeEvents(eventStreamId2, nameWasChanged1);
      writeEvents(eventStreamId3, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.all())
          .hasOnlyEvents(nameDefined, nameWasChanged1, nameWasChanged2);
    }

    @Test
    void all_with_skip_and_limit_returns_all_events_within_skip_and_limit() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.all(1, 2))
          .hasOnlyEvents(nameWasChanged1, nameWasChanged2);
    }

    @Test
    void query_with_single_filter_without_skip_and_limit() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String eventStreamId3 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);
      persist(eventStreamId3, CloudEventBuilder.v1()
          .withId(UUID.randomUUID().toString())
          .withSource(URI.create("http://something"))
          .withType("something")
          .withTime(LocalDateTime.now().atOffset(UTC))
          .withSubject("subject")
          .withDataContentType("application/json")
          .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
          .build()
      ).block(); //TODO: Change

      // Then
      thenEventQuery(eventStore.query(source(NAME_SOURCE)))
          .hasOnlyEvents(nameDefined, nameWasChanged1, nameWasChanged2);
    }

    @Test
    void query_with_single_filter_with_skip_and_limit() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);
      persist("something", CloudEventBuilder.v1()
          .withId(UUID.randomUUID().toString())
          .withSource(URI.create("http://something"))
          .withType("something")
          .withTime(LocalDateTime.now().atOffset(UTC))
          .withSubject("subject")
          .withDataContentType("application/json")
          .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
          .build()
      ).block();

      // Then
      thenEventQuery(eventStore.query(source(NAME_SOURCE), 1, 1))
          .hasOnlyEvents(nameWasChanged1);
    }


    @Test
    void compose_filters_using_and() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String nameWasDefinedEventId = anEventId();
      NameDefined nameDefined = new NameDefined(nameWasDefinedEventId, now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))).and(id(nameWasDefinedEventId))))
          .hasOnlyEvents(nameDefined);
    }

    @Test
    void compose_filters_using_or() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.query(time(OffsetDateTime.of(now.plusHours(2), UTC)).or(source(NAME_SOURCE))))
          .hasOnlyEvents(nameDefined, nameWasChanged1, nameWasChanged2);
    }

    @Test
    void query_filter_by_subject() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);


      // Then
      thenEventQuery(eventStore.query(subject("NameWasChanged")))
          .hasOnlyEvents(nameWasChanged1, nameWasChanged2);
    }

    //FIXME: Check!?
    // @Disabled("How to query data from SQL DB??? TODO FIXME")
    @Test
    void query_filter_by_data() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.query(data("name", eq("name2")).or(data("name", eq("name")))))
          .hasOnlyEvents(nameWasChanged1, nameDefined);
    }

    @Test
    void query_filter_by_cloud_event() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      String eventId = UUID.randomUUID().toString();
      NameWasChanged nameWasChanged1 = new NameWasChanged(eventId, now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.query(cloudEvent(eventId, NAME_SOURCE)))
          .hasOnlyEvents(nameWasChanged1);
    }

    @Test
    void query_filter_by_type() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      thenEventQuery(eventStore.query(type(NameDefined.class.getName())))
          .hasOnlyEvents(nameDefined);
    }

    //FIXME: Something wrong with data!?
    @Test
    void query_filter_by_data_schema() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);
      CloudEvent cloudEvent = CloudEventBuilder.v1()
          .withId(UUID.randomUUID().toString())
          .withSource(URI.create("http://something"))
          .withType("something")
          .withTime(LocalDateTime.now().atOffset(UTC))
          .withSubject("subject")
          .withDataSchema(URI.create("urn:myschema"))
          .withDataContentType("application/json")
          .withData("{\"hello\":\"world\"}".getBytes(UTF_8))
          .withExtension(OccurrentCloudEventExtension.occurrent("something", 1))
          .build();
      persist("something", cloudEvent).block();

      // Then
      Flux<CloudEvent> events = eventStore.query(dataSchema(URI.create("urn:myschema")));
      CloudEvent expectedCloudEvent = CloudEventBuilder.v1(cloudEvent).withData(PojoCloudEventData.wrap(Document.parse(new String(requireNonNull(cloudEvent.getData()).toBytes(), UTF_8)), document -> document.toJson().getBytes(UTF_8))).build();
      assertThat(events.toStream()).containsExactly(expectedCloudEvent);
    }

  }

  @Nested
  @DisplayName("time queries")
  class TimeQueries {

    @Test
    void query_filter_by_time_lt() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      Flux<CloudEvent> events = eventStore.query(time(lt(OffsetDateTime.of(now.plusHours(2), UTC))));
      thenEventQuery(events)
          .hasOnlyEvents(nameDefined, nameWasChanged1);
    }

    @Test
    void query_filter_by_time_range_is_wider_than_persisted_time_range() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now.plusMinutes(35), UTC)), lte(OffsetDateTime.of(now.plusHours(4), UTC)))));
      thenEventQuery(events)
          .hasOnlyEvents(nameWasChanged1, nameWasChanged2);
    }

    @Test
    void query_filter_by_time_range_has_exactly_the_same_range_as_persisted_time_range() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      Flux<CloudEvent> events = eventStore.query(time(and(gte(OffsetDateTime.of(now, UTC)), lte(OffsetDateTime.of(now.plusHours(2), UTC)))));
      thenEventQuery(events)
          .hasOnlyEvents(nameDefined, nameWasChanged1, nameWasChanged2);
    }

    @Test
    void query_filter_by_time_range_has_a_range_smaller_as_persisted_time_range() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(2), "name3");

      // When
      writeEvents(eventStreamId1, Arrays.asList(nameDefined, nameWasChanged1));
      writeEvents(eventStreamId2, nameWasChanged2);

      // Then
      Flux<CloudEvent> events = eventStore.query(time(and(gt(OffsetDateTime.of(now.plusMinutes(50), UTC)), lt(OffsetDateTime.of(now.plusMinutes(110), UTC)))));
      thenEventQuery(events)
          .hasOnlyEvents(nameWasChanged1);
    }

  }

  @Nested
  @DisplayName("sort")
  class SortTest { //TODO: Parameterized test

    @Test
    void sort_by_natural_asc() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String eventStreamId3 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

      // When
      writeEvents(eventStreamId3, nameWasChanged1);
      writeEvents(eventStreamId1, nameWasChanged2);
      writeEvents(eventStreamId2, nameDefined);

      // Then
      Flux<CloudEvent> events = eventStore.all(EventStoreQueries.SortBy.NATURAL_ASC);
      thenEventQuery(events)
          .hasOnlyEvents(nameWasChanged1, nameWasChanged2, nameDefined);
    }

    @Test
    void sort_by_natural_desc() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String eventStreamId3 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

      // When
      writeEvents(eventStreamId3, nameWasChanged1);
      writeEvents(eventStreamId1, nameWasChanged2);
      writeEvents(eventStreamId2, nameDefined);

      // Then
      Flux<CloudEvent> events = eventStore.all(EventStoreQueries.SortBy.NATURAL_DESC);
      thenEventQuery(events)
          .hasOnlyEvents(nameDefined, nameWasChanged2, nameWasChanged1);
    }

    @Test
    void sort_by_time_asc() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String eventStreamId3 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now, "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

      // When
      writeEvents(eventStreamId3, nameWasChanged1);
      writeEvents(eventStreamId1, nameWasChanged2);
      writeEvents(eventStreamId2, nameDefined);

      // Then
      Flux<CloudEvent> events = eventStore.all(EventStoreQueries.SortBy.TIME_ASC);
      thenEventQuery(events)
          .hasOnlyEvents(nameWasChanged1, nameDefined, nameWasChanged2);
    }

    @Test
    void sort_by_time_desc() {
      // Given
      LocalDateTime now = LocalDateTime.now();
      String eventStreamId1 = anStreamId();
      String eventStreamId2 = anStreamId();
      String eventStreamId3 = anStreamId();
      NameDefined nameDefined = new NameDefined(UUID.randomUUID().toString(), now.plusHours(3), "name");
      NameWasChanged nameWasChanged1 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(-2), "name2");
      NameWasChanged nameWasChanged2 = new NameWasChanged(UUID.randomUUID().toString(), now.plusHours(1), "name3");

      // When
      writeEvents(eventStreamId3, nameWasChanged1);
      writeEvents(eventStreamId1, nameWasChanged2);
      writeEvents(eventStreamId2, nameDefined);

      // Then
      Flux<CloudEvent> events = eventStore.all(EventStoreQueries.SortBy.TIME_DESC);
      thenEventQuery(events)
          .hasOnlyEvents(nameDefined, nameWasChanged2, nameWasChanged1);
    }

  }

  @Override
  public EventStore eventStore() {
    return this.eventStore;
  }

}
