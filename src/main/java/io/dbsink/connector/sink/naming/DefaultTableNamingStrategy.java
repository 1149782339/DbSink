/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

/**
 * Default  table naming strategy without any changes
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class DefaultTableNamingStrategy implements TableNamingStrategy {
    /**
     * Resolve table name from field name in sink record
     * {@link org.apache.kafka.connect.sink.SinkRecord}
     *
     * @author Wang Wei
     * @time: 2023-06-24
     */
    @Override
    public String resolveTableName(String table) {
        return table;
    }
}
