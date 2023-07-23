/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.event;

import io.dbsink.connector.sink.dialect.DatabaseType;

/**
 * Change Event equivalent to ChangeEvent.
 * See #io.debezium.engine.ChangeEvent
 *
 * @author: Wang Wei
 * @time: 2023-06-10
 */
public abstract class ChangeEvent {
    protected String topic;
    protected long offset;
    protected Integer partition;

    protected DatabaseType databaseType;

    public String getTopic() {
        return topic;
    }

    public long getOffset() {
        return offset;
    }

    public Integer getPartition() {
        return partition;
    }

    public abstract String getTransactionId();

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

}
