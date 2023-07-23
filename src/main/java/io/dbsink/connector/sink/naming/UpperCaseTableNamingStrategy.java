/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

import io.dbsink.connector.sink.relation.TableId;

import java.util.Locale;
import java.util.Optional;

/**
 * Upper case table naming strategy
 * source database with default lowercase to target database with default uppercase
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class UpperCaseTableNamingStrategy implements TableNamingStrategy {
    /**
     * Resolve table name from field name in sink record
     * {@link org.apache.kafka.connect.sink.SinkRecord}
     *
     * @author Wang Wei
     * @time: 2023-06-24
     */
    @Override
    public TableId resolveTableId(TableId tableId) {
        String catalog = Optional
            .ofNullable(tableId.getCatalog()).map(item -> item.toUpperCase(Locale.ROOT))
            .orElse(null);
        String schema = Optional
            .ofNullable(tableId.getSchema()).map(item -> item.toUpperCase(Locale.ROOT))
            .orElse(null);
        String table = Optional
            .ofNullable(tableId.getTable()).map(item -> item.toUpperCase(Locale.ROOT))
            .orElse(null);
        return new TableId(catalog, schema, table);
    }
}
