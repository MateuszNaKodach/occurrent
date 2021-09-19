package org.occurrent.eventstore.sql.spring.reactor;

import org.occurrent.eventstore.api.DuplicateCloudEventException;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;

class DataIntegrityViolationToDuplicateCloudEventExceptionTranslator {

  static DuplicateCloudEventException translateToDuplicateCloudEventException(DataIntegrityViolationException e) {
    return new DuplicateCloudEventException(null, null, Optional.ofNullable(e.getMessage()).map(String::trim).orElse(null), e);
  }
}
