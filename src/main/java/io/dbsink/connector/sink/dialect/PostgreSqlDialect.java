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
import io.dbsink.connector.sink.util.BooleanUtil;
import io.dbsink.connector.sink.util.ByteUtil;
import io.dbsink.connector.sink.util.DurationUtil;
import io.dbsink.connector.sink.util.StringUtil;
import org.apache.kafka.connect.data.Schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Postgres dialect, override common database dialect method {@link CommonDatabaseDialect}
 *
 * @author Wang Wei
 * @time: 2023-06-15
 */
public class PostgreSqlDialect extends CommonDatabaseDialect {
    public PostgreSqlDialect(ConnectorConfig config) {
        super(config);
    }


    /**
     * Get upsert sql statement.
     * Postgres upsert sql statement example:
     * Insert into "migration"."test1"("col1","col2") values(?,?) on conflict("col1")
     * do update set "col2"=excluded."col2";
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return upsert sql statement
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    @Override
    protected String getUpsertSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions) {
        final ExpressionBuilder expressionBuilder = createExpressionBuilder();
        final ExpressionBuilder.Transformer<ColumnDefinition> transformer =
            (builder,colDef) -> builder.append(colDef).append("=excluded.").append(colDef);
        expressionBuilder.append("insert into ")
            .append(tableId)
            .append("(")
            .listBuilder()
            .delimitedBy(",")
            .of(pkColumnDefinitions, nonPkColumnDefinitions);
        expressionBuilder.append(") ")
            .append("values(")
            .appendMulti("," , "?", pkColumnDefinitions.size() + nonPkColumnDefinitions.size())
            .append(") ")
            .append("on conflict(")
            .listBuilder()
            .delimitedBy(",")
            .of(pkColumnDefinitions)
            .append(")");
        if (nonPkColumnDefinitions.size() == 0) {
            expressionBuilder.append(" do nothing");
        } else {
            expressionBuilder.append(" do update set ")
                .listBuilder()
                .delimitedBy(",")
                .transformedBy(transformer)
                .of(nonPkColumnDefinitions);
        }
        return expressionBuilder.toString();
    }

    @Override
    public void executeDDL(TableId tableId, String ddl, Connection connection) throws SQLException {
        if (!StringUtil.isEmpty(tableId.getSchema())) {
            // postgres support switch schema
            // so first try to switch the schema
            // then execute ddl(ddl may not contain schema)
            connection.setSchema(tableId.getSchema());
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }


    /**
     * Normalize the value of SQLSTATE returned by postgres to a unified type {@link SQLState}
     *
     * @param sqlState value from {@link SQLException}
     * @return a unified enum type SQLSTATE {@link SQLState}
     * @author: Wang Wei
     * @time: 2023-06-18
     */
    @Override
    public SQLState resolveSQLState(String sqlState) {
        if ("23505".equals(sqlState)) {
            return SQLState.ERR_DUP_KEY;
        } else if ("42P07".equals(sqlState)) {
            return SQLState.ERR_RELATION_EXISTS_ERROR;
        } else if ("42P01".equals(sqlState)) {
            return SQLState.ERR_RELATION_NOT_EXISTS_ERROR;
        }
        return super.resolveSQLState(sqlState);
    }

    /**
     * Bind primitive field
     *
     * @param statement        prepared statement
     * @param value            value
     * @param schema           schema
     * @param columnDefinition column definition
     * @param index            column index
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    protected boolean bindPrimitive(
        PreparedStatement statement,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition,
        int index
    ) throws SQLException {
        if (schema.type() == null) {
            return false;
        }
        switch (schema.type()) {
            case INT16: {
                if ("bool".equals(columnDefinition.getTypeName())) {
                    statement.setBoolean(index, BooleanUtil.toBoolean((Short) value));
                } else {
                    statement.setShort(index, (Short) value);
                }
                return true;
            }
            default:
                return super.bindPrimitive(statement, value, schema, columnDefinition, index);
        }
    }
    @Override
    protected boolean bindDebezium(
        PreparedStatement statement,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition,
        int index
    ) throws SQLException {
        if (schema.name() == null) {
            return false;
        }
        switch (schema.name()) {
            case "io.debezium.time.MicroTime":
            case "io.debezium.time.MicroDuration": {
                if (columnDefinition.getJdbcType() == Types.TIME) {
                    statement.setString(index, DurationUtil.toTime(TimeUnit.MICROSECONDS.toNanos((Long) value)));
                } else {
                    statement.setString(
                        index,
                        DurationUtil.toInterval(TimeUnit.MICROSECONDS.toNanos((Long) value))
                    );
                }
                return true;
            }
            case "io.debezium.time.NanoDuration": {
                if (columnDefinition.getJdbcType() == Types.TIME) {
                    statement.setTime(
                        index,
                        new java.sql.Time(TimeUnit.NANOSECONDS.toMillis((Long) value)),
                        Calendar.getInstance(TimeZone.getTimeZone("GMT"))
                    );
                } else {
                    statement.setString(index, DurationUtil.toInterval((Long) value));
                }
                return true;
            }
            case "io.debezium.data.Bits": {
                byte[] bytes = (byte[]) value;
                if (bytes.length == 1 && columnDefinition.getJdbcType() == Types.BOOLEAN) {
                    statement.setBoolean(index, ByteUtil.byteToBoolean(bytes[0]));
                } else {
                    statement.setString(index, ByteUtil.bytesToBinaryString(bytes, columnDefinition.getLength()));
                }
                return true;
            }
            case "io.debezium.time.Year": {
                int year = (Integer) value;
                int jdbcType = columnDefinition.getJdbcType();
                if (jdbcType == Types.INTEGER || jdbcType == Types.BIGINT || jdbcType == Types.SMALLINT) {
                    // int type
                    statement.setInt(index, year);
                } else {
                    // interval type
                    statement.setString(index, year + " year");
                }
                return true;
            }
            case "io.debezium.time.Interval": {
                statement.setString(index, (String) value);
                return true;
            }
            default:
                return super.bindDebezium(statement, value, schema, columnDefinition, index);
        }
    }

    protected boolean bindLogical(
        PreparedStatement statement,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition,
        int index
    ) throws SQLException {
        if (schema.name() == null) {
            return super.bindLogical(statement, value, schema, columnDefinition, index);
        }
        switch (schema.name()) {
            case org.apache.kafka.connect.data.Time.LOGICAL_NAME: {
                if (columnDefinition.getJdbcType() == Types.TIME) {
                    // time
                    statement.setTime(
                        index,
                        new java.sql.Time(((java.util.Date) value).getTime()),
                        Calendar.getInstance(TimeZone.getTimeZone("GMT"))
                    );
                } else {
                    // interval
                    statement.setString(
                        index,
                        DurationUtil.toInterval(TimeUnit.SECONDS.toNanos(((java.util.Date) value).getTime()))
                    );
                }
                return true;
            }
            default:
                return super.bindLogical(statement, value, schema, columnDefinition, index);
        }
    }
    @Override
    protected Properties getJdbcProperties() {
        Properties properties = new Properties();
        properties.put("stringtype", "unspecified");
        return properties;
    }

    @Override
    public TableId resolveTableId(TableId tableId) {
        final String catalog = tableId.getCatalog();
        final String schema = tableId.getSchema();
        final String table = tableId.getTable();
        return new TableId(null, schema != null ? schema : catalog, table);
    }

    @Override
    public DatabaseType databaseType() {
        return DatabaseType.POSTGRES;
    }

    public static class PostgreSqlDialectProvider implements DatabaseDialectProvider {

        @Override
        public DatabaseDialect create(ConnectorConfig config) {
            return new PostgreSqlDialect(config);
        }

        @Override
        public String dialectName() {
            return "PostgreSqlDialect";
        }
    }
}
