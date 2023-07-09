/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

import java.util.Locale;

public class LowCaseColumnNamingStrategy implements ColumnNamingStrategy {
    /**
     * Resolve column name from field name in sink record
     * {@link org.apache.kafka.connect.sink.SinkRecord}
     * convert it to lower case,
     *
     * @author Wang Wei
     * @time: 2023-06-24
     */
    @Override
    public String resolveColumnName(String column) {
        return column.toLowerCase(Locale.ROOT);
    }
}
