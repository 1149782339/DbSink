/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.event;

import io.dbsink.connector.sink.dialect.DatabaseType;
import io.dbsink.connector.sink.relation.TableId;

/**
 * Schema Change Event equivalent to SchemaChangeEvent.
 * See #io.debezium.schema.SchemaChangeEvent
 *
 * @author: Wang Wei
 * @time: 2023-06-10
 */
public class SchemaChangeEvent extends ChangeEvent {
    private String ddl;
    private TableId tableId;

    private String transactionId;

    public String getDDL() {
        return ddl;
    }

    public TableId getTableId() {
        return tableId;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    public static final class Builder {
        private String ddl;
        private TableId tableId;
        private String topic;
        private long offset;
        private Integer partition;

        private DatabaseType databaseType;

        private String transactionId;

        private Builder() {
        }


        public Builder ddl(String ddl) {
            this.ddl = ddl;
            return this;
        }

        public Builder tableId(TableId tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder partition(Integer partition) {
            this.partition = partition;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder databaseType(DatabaseType databaseType) {
            this.databaseType = databaseType;
            return this;
        }

        public SchemaChangeEvent build() {
            SchemaChangeEvent schemaChangeEvent = new SchemaChangeEvent();
            schemaChangeEvent.tableId = this.tableId;
            schemaChangeEvent.partition = this.partition;
            schemaChangeEvent.ddl = this.ddl;
            schemaChangeEvent.offset = this.offset;
            schemaChangeEvent.topic = this.topic;
            schemaChangeEvent.databaseType = this.databaseType;
            return schemaChangeEvent;
        }
    }
}
