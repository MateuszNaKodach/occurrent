package org.occurrent.eventstore.sql.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.occurrent.domain.DomainEvent;
import org.occurrent.eventstore.api.WriteCondition;
import org.occurrent.eventstore.api.reactor.EventStore;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.occurrent.time.TimeConversion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.time.ZoneOffset.UTC;
import static org.occurrent.eventstore.sql.spring.reactor.Constants.NAME_SOURCE;


interface ReactorEventStoreTestSupport {

  EventStore eventStore();

  default void writeEvents(String eventStreamId, List<DomainEvent> events) {
    writeEvents(eventStreamId, events, WriteCondition.anyStreamVersion());
  }

  default void writeEvents(String eventStreamId, DomainEvent events) {
    writeEvents(eventStreamId, events, WriteCondition.anyStreamVersion());
  }

  default void writeEvents(String eventStreamId, List<DomainEvent> events, WriteCondition writeCondition) {
    StepVerifier.create(persist(eventStreamId, writeCondition, events))
        .verifyComplete();
  }

  default void writeEvents(String eventStreamId, DomainEvent events, WriteCondition writeCondition) {
    StepVerifier.create(persist(eventStreamId, writeCondition, Collections.singletonList(events)))
        .verifyComplete();
  }

  default EventStreamAssertions thenEventStream(String eventStreamId) {
    return EventStreamAssertions.thenEventStream(this.eventStore().read(eventStreamId));
  }

  default EventStreamAssertions thenEventStream(Mono<EventStream<CloudEvent>> eventStream) {
    return EventStreamAssertions.thenEventStream(eventStream);
  }

  default String anStreamId() {
    return UUID.randomUUID().toString();
  }

  default String anEventId() {
    return UUID.randomUUID().toString();
  }


  //TODO: Duplication below - same as ReactorMongoEventStore
  default Mono<Void> persist(String eventStreamId, CloudEvent event) {
    return eventStore().write(eventStreamId, Flux.just(event));
  }

  default Mono<Void> persist(String eventStreamId, DomainEvent event) {
    return eventStore().write(eventStreamId, Flux.just(convertDomainEventCloudEvent(event)));
  }

  default Mono<Void> persist(String eventStreamId, Flux<DomainEvent> events) {
    return eventStore().write(eventStreamId, events.map(this::convertDomainEventCloudEvent));
  }

  default Mono<Void> persist(String eventStreamId, List<DomainEvent> events) {
    return persist(eventStreamId, Flux.fromIterable(events));
  }

  default Mono<Void> persist(String eventStreamId, WriteCondition writeCondition, DomainEvent event) {
    List<DomainEvent> events = new ArrayList<>();
    events.add(event);
    return persist(eventStreamId, writeCondition, events);
  }

  default Mono<Void> persist(String eventStreamId, WriteCondition writeCondition, List<DomainEvent> events) {
    return persist(eventStreamId, writeCondition, Flux.fromIterable(events));
  }

  default Mono<Void> persist(String eventStreamId, WriteCondition writeCondition, Flux<DomainEvent> events) {
    return eventStore().write(eventStreamId, writeCondition, events.map(this::convertDomainEventCloudEvent));
  }

  default CloudEvent convertDomainEventCloudEvent(DomainEvent domainEvent) {
    return CloudEventBuilder.v1()
        .withId(domainEvent.getEventId())
        .withSource(NAME_SOURCE)
        .withType(domainEvent.getClass().getName())
        .withTime(TimeConversion.toLocalDateTime(domainEvent.getTimestamp()).atOffset(UTC))
        .withSubject(domainEvent.getClass().getSimpleName())
        .withDataContentType("application/json")
        .withData(serializeEvent(domainEvent))
        .build();
  }

  default byte[] serializeEvent(DomainEvent domainEvent) {
    ObjectMapper objectMapper = new ObjectMapper();
    return CheckedFunction.unchecked(objectMapper::writeValueAsBytes).apply(domainEvent);
  }

  default void await(CountDownLatch countDownLatch) {
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
