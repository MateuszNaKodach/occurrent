package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.domain.DomainEvent;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.testcontainers.shaded.com.google.common.base.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.occurrent.eventstore.sql.spring.reactor.CloudEventsDeserializer.deserialize;

class EventQueryAssertions {

  private final Flux<CloudEvent> eventsQuery;

  private EventQueryAssertions(Flux<CloudEvent> eventsQuery) {
    this.eventsQuery = eventsQuery;
  }

  static EventQueryAssertions thenEventQuery(Flux<CloudEvent> eventStream) {
    return new EventQueryAssertions(eventStream);
  }

  EventQueryAssertions hasOnlyEvents(DomainEvent... events) {
    return hasOnlyEvents(Arrays.stream(events).collect(Collectors.toList()));
  }

  EventQueryAssertions hasOnlyEvent(DomainEvent event) {
    return hasOnlyEvents(Collections.singletonList(event));
  }

  EventQueryAssertions hasOnlyEvents(List<DomainEvent> events) {
    StepVerifier.create(eventsQuery.map(deserialize()))
        .expectNextSequence(events)
        .verifyComplete();
    return this;
  }

  EventQueryAssertions hasNoEvents() {
    StepVerifier.create(eventsQuery.map(deserialize()))
        .verifyComplete();
    return this;
  }

}
