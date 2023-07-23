/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.dialect;

import io.dbsink.connector.sink.annotation.ThreadSafe;
import io.dbsink.connector.sink.jdbc.StatementBinder;
import io.dbsink.connector.sink.relation.ColumnDefinition;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableDefinition;
import io.dbsink.connector.sink.relation.TableId;
import io.dbsink.connector.sink.sql.SQLState;
import org.apache.kafka.connect.data.Schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Database dialect interface
 * it is used by jdbc applier to read the table struct and
 * do insert、delete、update、upsert by using jdbc interface
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
@ThreadSafe
public interface DatabaseDialect {

    /**
     * Build insert sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return insert sql statement
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    String buildInsertStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition);

    /**
     * Build update sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return update sql statement
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    String buildUpdateStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition);

    /**
     * Build upsert sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return upsert sql statement
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    String buildUpsertStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition);

    /**
     * Build delete sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return delete sql statement
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    String buildDeleteStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition);

    PreparedStatement createPreparedStatement(String sql, Connection connection) throws SQLException;

    /**
     * Execute ddl sql statement
     *
     * @param tableId    table identifier {@link TableId}
     * @param ddl        ddl sql statement
     * @param connection jdbc connection {@link Connection}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void executeDDL(TableId tableId, String ddl, Connection connection) throws SQLException;

    /**
     * Create statement binder for binding params
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return statement binder {@link StatementBinder}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    StatementBinder createStatementBinder(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition);

    /**
     * Judge if one table exists
     *
     * @param tableId table identifier {@link TableId}
     * @return if the table exists
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    boolean tableExists(Connection connection, TableId tableId) throws SQLException;

    /**
     * Read one table definition
     *
     * @return table definition {@link TableDefinition}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    TableDefinition readTable(Connection connection, TableId tableId) throws SQLException;

    /**
     * Get Connection
     *
     * @return connection {@link Connection}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    Connection getConnection() throws SQLException;

    /**
     * Normalize the value of SQLSTATE returned by each database to a unified type {@link SQLState}
     * for SQLException {@link SQLException} handling, like duplicate key,relation not exist and so on
     *
     * @param sqlState value from {@link SQLException}
     * @return a unified enum type SQLSTATE {@link SQLState}
     * @author: Wang Wei
     * @time: 2023-06-18
     */
    SQLState resolveSQLState(String sqlState);

    /**
     * resolve table identifier from the source database
     * convert it to the table identifier in the target database
     *
     * @param tableId table identifier(catalog,schema,table) {@link TableId}
     * @return table identifier in the target database {@link TableId}
     * @author: Wang Wei
     * @time: 2023-06-18
     */
    TableId resolveTableId(TableId tableId);

    /**
     * Bind one field from sink record,
     * this method uses jdbc interface setXXX like setString,setTimestamp and so on,
     * which depends on schema and column definition
     *
     * @param statement        prepared statement {@link PreparedStatement}
     * @param value            bind value, may be null {@link Object}
     * @param schema           schema, not null {@link Schema}
     * @param columnDefinition col definition, not null {@link ColumnDefinition}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-18
     */
    void bindField(
        PreparedStatement statement,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition,
        int index
    ) throws SQLException;

    DatabaseType databaseType();
}
