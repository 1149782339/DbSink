/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

import io.dbsink.connector.sink.relation.TableId;

import java.util.Locale;

/**
 * Low case table naming strategy
 * source database with default uppercase  to target database with default lowercase
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class LowCaseTableNamingStrategy implements TableNamingStrategy {
    /**
     * Resolve table name from field name in sink record
     * {@link org.apache.kafka.connect.sink.SinkRecord}
     *
     * @author Wang Wei
     * @time: 2023-06-24
     */
    @Override
    public TableId resolveTableId(TableId tableId) {
        return null;
    }
}
