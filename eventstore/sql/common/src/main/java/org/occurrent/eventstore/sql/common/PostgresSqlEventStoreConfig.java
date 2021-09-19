package org.occurrent.eventstore.sql.common;

public class PostgresSqlEventStoreConfig implements SqlEventStoreConfig {

  private final String eventStoreTableName;

  public PostgresSqlEventStoreConfig(String eventStoreTableName) {
    this.eventStoreTableName = eventStoreTableName;
  }

  @Override
  public String eventStoreTableName() {
    return this.eventStoreTableName;
  }

  //TODO: Allow to configure if data is jsonb or bytea
  @Override
  public String createEventStoreTableSql() {
    return "CREATE TABLE IF NOT EXISTS " + eventStoreTableName + "("
        + "id VARCHAR(255) NOT NULL," + "\n"
        + "source VARCHAR(255) NOT NULL," + "\n"
        + "specversion VARCHAR(255) NOT NULL," + "\n"
        + "type VARCHAR(255) NOT NULL," + "\n"
        + "datacontenttype VARCHAR(255)," + "\n"
        + "dataschema VARCHAR(255)," + "\n"
        + "subject VARCHAR(255)," + "\n"
        + "streamid VARCHAR(255)," + "\n"
        + "streamversion BIGINT," + "\n"
        + "data bytea," + "\n"
        + "time TIMESTAMPTZ," + "\n"
        + "PRIMARY KEY (id, source)," + "\n"
        + "UNIQUE (streamid, streamversion)" + "\n"
        + ")";
  }
}
