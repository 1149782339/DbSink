/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.dialect;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.relation.ColumnDefinition;
import io.dbsink.connector.sink.relation.TableId;
import io.dbsink.connector.sink.sql.ExpressionBuilder;
import io.dbsink.connector.sink.sql.SQLState;
import io.dbsink.connector.sink.util.StringUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Mysql dialect, override common database dialect method {@link CommonDatabaseDialect}
 *
 * @author Wang Wei
 * @time: 2023-06-11
 */
public class MySqlDialect extends CommonDatabaseDialect {

    public MySqlDialect(ConnectorConfig config) {
        super(config);
    }


    /**
     * Get upsert sql statement.
     * Mysql upsert sql statement example:
     * Insert into "migration"."test1"("col1","col2") values(?,?)
     * on duplicate key update do update set "col2"=excluded."col2";
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return upsert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    protected String getUpsertSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions) {
        final ExpressionBuilder expressionBuilder = createExpressionBuilder();
        final ExpressionBuilder.Transformer<ColumnDefinition> transformer = (builder, columnDef) ->
            builder.append(columnDef).append("=").append(columnDef);
        expressionBuilder.append("insert into ")
            .append(tableId)
            .append("(")
            .listBuilder()
            .delimitedBy(",")
            .of(pkColumnDefinitions, nonPkColumnDefinitions);
        expressionBuilder.append(")")
            .append(" values(")
            .appendMulti(",", "?", pkColumnDefinitions.size() + nonPkColumnDefinitions.size())
            .append(") ")
            .append("on duplicate key update ")
            .listBuilder()
            .delimitedBy(",")
            .transformedBy(transformer)
            .of(!nonPkColumnDefinitions.isEmpty() ? nonPkColumnDefinitions : pkColumnDefinitions);
        return expressionBuilder.toString();
    }

    /**
     * Execute ddl sql statement.
     * Mysql can switch to another database(catalog) in one connection, and ddl from Debezium
     * may not contain database(catalog), therefore we should switch to the database
     * before executing ddl
     *
     * @param tableId    table identifier {@link TableId}
     * @param ddl        ddl sql statement
     * @param connection jdbc connection {@link Connection}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public void executeDDL(TableId tableId, String ddl, Connection connection) throws SQLException {
        if (!StringUtil.isEmpty(tableId.getCatalog())) {
            // mysql supports database and doesn't support schema
            // mysql can switch other database in one connection,
            // so first try to switch the database if possible
            // then execute ddl
            // it is because that ddl does not always contain database
            connection.setCatalog(tableId.getCatalog());
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }

    /**
     * Normalize the value of SQLSTATE returned by mysql to a unified type {@link SQLState}
     *
     * @param sqlState value from {@link SQLException}
     * @return a unified enum type SQLSTATE {@link SQLState}
     * @author: Wang Wei
     * @time: 2023-06-18
     */
    @Override
    public SQLState resolveSQLState(String sqlState) {
        if ("42S01".equals(sqlState)) {
            return SQLState.ERR_RELATION_EXISTS_ERROR;
        } else if ("42S02".equals(sqlState)) {
            return SQLState.ERR_RELATION_NOT_EXISTS_ERROR;
        }
        return super.resolveSQLState(sqlState);
    }

    /**
     * Resolve table identifier(catalog,schema,table) {@link TableId} from the source database
     * convert it to the table identifier in the mysql. There is no schema in mysql, so schema
     * is always null in table identifier {@link TableId}.
     *
     * @param tableId table identifier(catalog,schema,table) {@link TableId}
     * @return table identifier in the target database {@link TableId}
     * @author: Wang Wei
     * @time: 2023-06-18
     */
    @Override
    public TableId resolveTableId(TableId tableId) {
        final String catalog = tableId.getCatalog();
        final String schema = tableId.getSchema();
        final String table = tableId.getTable();
        return new TableId(schema != null ? schema : catalog, null, table);
    }

    @Override
    public DatabaseType databaseType() {
        return DatabaseType.MYSQL;
    }

    @Override
    protected Properties getJdbcProperties() {
        Properties properties = new Properties();
        properties.put("serverTimezone", "GMT");
        return properties;
    }

    public static class MySqlDialectProvider implements DatabaseDialectProvider {

        @Override
        public DatabaseDialect create(ConnectorConfig config) {
            return new MySqlDialect(config);
        }

        @Override
        public String dialectName() {
            return "MySqlDialect";
        }
    }
}
