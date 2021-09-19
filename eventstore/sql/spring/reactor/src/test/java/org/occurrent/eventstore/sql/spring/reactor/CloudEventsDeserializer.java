package org.occurrent.eventstore.sql.spring.reactor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import org.occurrent.domain.DomainEvent;
import org.occurrent.functional.CheckedFunction;

import java.util.function.Function;

class CloudEventsDeserializer {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @SuppressWarnings({"unchecked", "ConstantConditions"})
  static <T extends DomainEvent> T deserialize(CloudEvent cloudEvent) {
    try {
      return (T) objectMapper.readValue(cloudEvent.getData().toBytes(), Class.forName(cloudEvent.getType()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static Function<CloudEvent, DomainEvent> deserialize() {
    return CheckedFunction.unchecked(CloudEventsDeserializer::deserialize);
  }
}
