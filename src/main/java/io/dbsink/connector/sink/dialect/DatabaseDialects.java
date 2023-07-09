/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.dialect;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.annotation.ThreadSafe;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Database dialects, loads all dialects and
 * provides an interface to create a dialect
 *
 * @author Wang Wei
 * @time: 2023-06-23
 */
@ThreadSafe
public class DatabaseDialects {

    final static Map<String, DatabaseDialectProvider> DATABASE_DIALECT_REGISTRY = new LinkedHashMap<>();

    // load all database dialect providers
    static {
        loadDatabaseDialects();
    }

    private static void loadDatabaseDialects() {
        for (DatabaseDialectProvider databaseDialectProvider : ServiceLoader.load(DatabaseDialectProvider.class)) {
            DATABASE_DIALECT_REGISTRY.put(databaseDialectProvider.dialectName(), databaseDialectProvider);
        }
    }

    /**
     * create database dialect {@link DatabaseDialect} instance
     * with the given config {@link ConnectorConfig}
     *
     * @param config connector config
     * @return database dialect
     * @author: WANGWEI
     * @time: 2023-06-15
     */
    public static DatabaseDialect create(ConnectorConfig config) {
        final String dialectName = config.getDatabaseDialectName();
        DatabaseDialectProvider dialectProvider = DATABASE_DIALECT_REGISTRY.get(dialectName);
        if (dialectProvider == null) {
            throw new ConnectException("database dialect \"" + dialectName + "\" is not found!");
        }
        return dialectProvider.create(config);
    }
}
