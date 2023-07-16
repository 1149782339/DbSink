/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.connection;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.dialect.DatabaseDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Jdbc connection
 *
 * @author: Wang Wei
 * @time: 2023-06-14
 */
public class JdbcConnection {
    private final Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    private final DatabaseDialect databaseDialect;

    private final ConnectorConfig config;
    private volatile Connection connection;

    public JdbcConnection(DatabaseDialect databaseDialect, ConnectorConfig config) {
        this.databaseDialect = databaseDialect;
        this.config = config;
    }

    /**
     * get a valid connection
     *
     * @throws SQLException
     * @author: Wang Wei
     * @time: 2023-06-14
     */
    public synchronized Connection connection() throws SQLException {
        if (connection == null) {
            connection = databaseDialect.getConnection();
            return connection;
        }
        if (isValid(connection)) {
            return connection;
        }
        close(connection);
        connection = databaseDialect.getConnection();
        return connection;
    }

    private boolean isValid(Connection connection) {
        try {
            return connection.isValid(3000);
        } catch (SQLException e) {
            close(connection);
            return false;
        }
    }

    /**
     * close connection
     *
     * @author: Wang Wei
     * @time: 2023-06-14
     */
    public void close() {
        if (connection == null) {
            return;
        }
        close(connection);
    }

    private void close(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            LOGGER.trace("failed to close connection, ignore it", e);
        }
    }
}
