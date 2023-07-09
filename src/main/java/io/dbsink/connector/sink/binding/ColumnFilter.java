/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.binding;

import io.dbsink.connector.sink.relation.ColumnDefinition;

import java.util.Optional;

/**
 * Column Filter, used to filter out unnecessary columns
 * like system column or generated column
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
@FunctionalInterface
public interface ColumnFilter {
    /**
     * Build upsert sql statement
     *
     * @param columnDefinition column definition {@link ColumnDefinition}
     * @return a column definition or empty if the column should be filtered out
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    Optional<ColumnDefinition> apply(ColumnDefinition columnDefinition);
}
