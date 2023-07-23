/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.relation;

import io.dbsink.connector.sink.annotation.ThreadSafe;
import io.dbsink.connector.sink.dialect.DatabaseDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Table definitions
 *
 * @author Wang Wei
 * @time: 2023-06-21
 */
@ThreadSafe
public class TableDefinitions {

    private final Map<TableId, TableDefinition> tableDefinitionCache;
    private final DatabaseDialect databaseDialect;

    /**
     * Table Definitions
     *
     * @param databaseDialect {@link DatabaseDialect}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    public TableDefinitions(DatabaseDialect databaseDialect) {
        this.databaseDialect = databaseDialect;
        this.tableDefinitionCache = new HashMap<>();
    }

    /**
     * Get table definition corresponding to the tableId
     *
     * @param connection {@link Connection}
     * @param tableId    {@link TableId}
     * @return table definition corresponding to the tableId  {@link TableDefinition}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    public synchronized TableDefinition get(Connection connection, TableId tableId) throws SQLException {
        TableDefinition tableDefinition = tableDefinitionCache.get(tableId);
        if (tableDefinition != null) {
            return tableDefinition;
        }
        if (!databaseDialect.tableExists(connection, tableId)) {
            return null;
        }
        tableDefinition = databaseDialect.readTable(connection, tableId);
        if (tableDefinition == null) {
            return null;
        }
        tableDefinitionCache.put(tableId, tableDefinition);
        return tableDefinition;
    }

    public synchronized void refresh(Connection connection, TableId tableId) throws SQLException {
        tableDefinitionCache.remove(tableId);
        TableDefinition tableDefinition = databaseDialect.readTable(connection, tableId);
        if (tableDefinition == null) {
            return;
        }
        tableDefinitionCache.put(tableId, tableDefinition);
    }
}
