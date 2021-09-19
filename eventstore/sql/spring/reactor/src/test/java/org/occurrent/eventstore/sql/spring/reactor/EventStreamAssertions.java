package org.occurrent.eventstore.sql.spring.reactor;

import io.cloudevents.CloudEvent;
import org.occurrent.domain.DomainEvent;
import org.occurrent.eventstore.api.reactor.EventStream;
import org.occurrent.functional.CheckedFunction;
import org.testcontainers.shaded.com.google.common.base.Supplier;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.occurrent.eventstore.sql.spring.reactor.CloudEventsDeserializer.deserialize;

class EventStreamAssertions {

  private final Mono<EventStream<CloudEvent>> eventStream;

  private EventStreamAssertions(Mono<EventStream<CloudEvent>> eventStream) {
    this.eventStream = eventStream;
  }

  static EventStreamAssertions thenEventStream(Supplier<Mono<EventStream<CloudEvent>>> eventStream) {
    return thenEventStream(eventStream.get());
  }

  static EventStreamAssertions thenEventStream(Mono<EventStream<CloudEvent>> eventStream) {
    return new EventStreamAssertions(eventStream);
  }

  EventStreamAssertions hasVersion(Integer expectedVersion) {
    return hasVersion(expectedVersion.longValue());
  }

  EventStreamAssertions hasVersion(Long expectedVersion) {
    StepVerifier.create(eventStream.map(EventStream::version))
        .expectNext(expectedVersion)
        .verifyComplete();
    return this;
  }

  EventStreamAssertions hasOnlyEvents(DomainEvent... events) {
    return hasOnlyEvents(Arrays.stream(events).collect(Collectors.toList()));
  }

  EventStreamAssertions hasOnlyEvent(DomainEvent event) {
    return hasOnlyEvents(Collections.singletonList(event));
  }

  EventStreamAssertions hasOnlyEvents(List<DomainEvent> events) {
    StepVerifier.create(eventStream.flatMapMany(EventStream::events).map(deserialize()))
        .expectNextSequence(events)
        .verifyComplete();
    return this;
  }

  EventStreamAssertions hasNoEvents() {
    StepVerifier.create(eventStream.flatMapMany(EventStream::events).map(deserialize()))
        .verifyComplete();
    return this;
  }

  void notExist() {
    hasVersion(0);
    hasNoEvents();
  }


}
