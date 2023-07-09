/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.jdbc;


import io.dbsink.connector.sink.event.DataChangeEvent;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * statement binder defines the interface for DML statement for binding parameters
 *
 * @author Wang Wei
 * @time: 2023-06-24
 */
public interface StatementBinder {

    /**
     * Bind parameters for insert statement
     *
     * @param statement {@link PreparedStatement}
     * @param event     {@link DataChangeEvent}
     * @return the equivalent java sql Timestamp {@link Timestamp}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void bindInsertStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException;

    /**
     * Bind parameters for update statement
     *
     * @param statement {@link PreparedStatement}
     * @param event     {@link DataChangeEvent}
     * @return the equivalent java sql Timestamp {@link Timestamp}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void bindUpdateStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException;

    /**
     * Bind parameters for delete statement
     *
     * @param statement {@link PreparedStatement}
     * @param event     {@link DataChangeEvent}
     * @return the equivalent java sql Timestamp {@link Timestamp}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void bindDeleteStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException;

    /**
     * Bind parameters for upsert statement
     *
     * @param statement {@link PreparedStatement}
     * @param event     {@link DataChangeEvent}
     * @return the equivalent java sql Timestamp {@link Timestamp}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void bindUpsertStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException;
}
