/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.dialect;

import io.dbsink.connector.sink.ConnectorConfig;

/**
 * Database dialect factory class
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public interface DatabaseDialectProvider {
    /**
     * Create database dialect
     *
     * @param config connector config {@link ConnectorConfig}
     * @return database dialect {@link DatabaseDialect}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    DatabaseDialect create(ConnectorConfig config);
    /**
     * Database dialect name
     *
     * @return database dialect name {@link String}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    String dialectName();
}
