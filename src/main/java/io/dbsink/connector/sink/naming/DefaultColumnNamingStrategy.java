/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.naming;

/**
 * Default column naming strategy without any changes
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public class DefaultColumnNamingStrategy implements ColumnNamingStrategy {
    @Override
    public String resolveColumnName(String column) {
        return column;
    }
}
