/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.event;

import io.dbsink.connector.sink.dialect.DatabaseType;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableId;

import java.util.Map;

/**
 * Data Change Event equivalent to DataChangeEvent.
 * See #io.debezium.pipeline.DataChangeEvent
 *
 * @author: Wang Wei
 * @time: 2023-06-10
 */
public class DataChangeEvent extends ChangeEvent {
    private Operation operation;
    private Map<String, Object> beforeValues;
    private Map<String, Object> afterValues;

    private FieldsMetaData fieldsMetaData;
    private TableId tableId;

    private String transactionId;

    public Operation getOperation() {
        return operation;
    }

    public Map<String, Object> getBeforeValues() {
        return beforeValues;
    }

    public Object getBeforeValue(String fieldName) {
        return beforeValues.get(fieldName);
    }

    public Map<String, Object> getAfterValues() {
        return afterValues;
    }

    public Object getAfterValue(String fieldName) {
        return afterValues.get(fieldName);
    }

    public FieldsMetaData getFieldsMetaData() {
        return fieldsMetaData;
    }

    public TableId getTableId() {
        return tableId;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    public static Builder builder() {
        return new Builder();
    }


    public static final class Builder {
        private Operation operation;
        private Map<String, Object> beforeValues;
        private Map<String, Object> afterValues;
        private TableId tableId;
        private String topic;
        private long offset;
        private Integer partition;

        private String transactionId;

        private FieldsMetaData fieldsMetaData;

        private DatabaseType databaseType;

        private Builder() {
        }


        public Builder operation(Operation operation) {
            this.operation = operation;
            return this;
        }

        public Builder beforeValues(Map<String, Object> beforeValues) {
            this.beforeValues = beforeValues;
            return this;
        }

        public Builder afterValues(Map<String, Object> afterValues) {
            this.afterValues = afterValues;
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

        public Builder partition(int partition) {
            this.partition = partition;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder fieldsMetaData(FieldsMetaData fieldsMetaData) {
            this.fieldsMetaData = fieldsMetaData;
            return this;
        }

        public Builder databaseType(DatabaseType databaseType) {
            this.databaseType = databaseType;
            return this;
        }

        public DataChangeEvent build() {
            DataChangeEvent dataChangeEvent = new DataChangeEvent();
            dataChangeEvent.fieldsMetaData = this.fieldsMetaData;
            dataChangeEvent.beforeValues = this.beforeValues;
            dataChangeEvent.partition = this.partition;
            dataChangeEvent.offset = this.offset;
            dataChangeEvent.tableId = this.tableId;
            dataChangeEvent.operation = this.operation;
            dataChangeEvent.afterValues = this.afterValues;
            dataChangeEvent.topic = this.topic;
            dataChangeEvent.transactionId = this.transactionId;
            dataChangeEvent.databaseType = this.databaseType;
            return dataChangeEvent;
        }
    }
}
