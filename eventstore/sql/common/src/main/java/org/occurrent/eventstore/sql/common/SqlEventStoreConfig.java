package org.occurrent.eventstore.sql.common;

public interface SqlEventStoreConfig {

  String createEventStoreTableSql();
  String eventStoreTableName();

}
