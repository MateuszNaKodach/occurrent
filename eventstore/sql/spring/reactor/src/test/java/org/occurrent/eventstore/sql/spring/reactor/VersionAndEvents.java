package org.occurrent.eventstore.sql.spring.reactor;

import org.occurrent.domain.DomainEvent;

import java.util.List;

class VersionAndEvents {
  final long version;
  final List<DomainEvent> events;

  VersionAndEvents(long version, List<DomainEvent> events) {
    this.version = version;
    this.events = events;
  }

  @Override
  public String toString() {
    return "VersionAndEvents{" +
        "version=" + version +
        ", events=" + events +
        '}';
  }
}
