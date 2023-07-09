/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

/**
 * Column naming strategy,used to resolve column name in target database
 * from kafka sink record{@link org.apache.kafka.connect.sink.SinkRecord}
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public interface ColumnNamingStrategy {
    /**
     * Resolve column name from field name in sink record
     * {@link org.apache.kafka.connect.sink.SinkRecord}
     *
     * @author Wang Wei
     * @time: 2023-06-24
     */
    String resolveColumnName(String column);
}
