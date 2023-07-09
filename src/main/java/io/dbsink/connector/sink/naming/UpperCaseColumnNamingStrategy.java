/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

import java.util.Locale;

/**
 * Upper case column naming strategy
 * source database with default lowercase  to target database with default uppercase
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class UpperCaseColumnNamingStrategy implements ColumnNamingStrategy {
    /**
     * Resolve column name from field name in sink record
     * {@link org.apache.kafka.connect.sink.SinkRecord}
     *
     * @author Wang Wei
     * @time: 2023-06-24
     */
    @Override
    public String resolveColumnName(String column) {
        return column.toUpperCase(Locale.ROOT);
    }
}
