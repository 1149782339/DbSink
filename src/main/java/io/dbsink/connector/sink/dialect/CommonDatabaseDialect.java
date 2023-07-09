/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.dialect;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.filter.ColumnFilter;
import io.dbsink.connector.sink.jdbc.PreparedStatementBinder;
import io.dbsink.connector.sink.jdbc.StatementBinder;
import io.dbsink.connector.sink.naming.ColumnNamingStrategy;
import io.dbsink.connector.sink.relation.ColumnDefinition;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableDefinition;
import io.dbsink.connector.sink.relation.TableId;
import io.dbsink.connector.sink.sql.ExpressionBuilder;
import io.dbsink.connector.sink.sql.IdentifierRule;
import io.dbsink.connector.sink.sql.SQLState;
import io.dbsink.connector.sink.util.DateTimeUtil;
import io.dbsink.connector.sink.util.StringUtil;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import org.apache.kafka.connect.errors.ConnectException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Common Database dialect, implements common methods {@link DatabaseDialect} for various database dialects
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public abstract class CommonDatabaseDialect implements DatabaseDialect {
    /**
     * Column naming strategy
     */
    protected final ColumnNamingStrategy columnNamingStrategy;
    /**
     * Jdbc url
     */
    protected final String jdbcUrl;
    /**
     * Jdbc username
     */
    protected final String jdbcUsername;
    /**
     * Jdbc password
     */
    protected final String jdbcPassword;
    /**
     * Jdbc driver class
     */
    protected final String jdbcDriverClass;
    /**
     * Time zone
     */
    protected final TimeZone timeZone;
    /**
     * Calendar
     */
    protected final Calendar calendar;
    /**
     * Identifier rule
     */
    protected final IdentifierRule identifierRule;

    protected final ColumnFilter columnFilter;

    public CommonDatabaseDialect(ConnectorConfig config) {
        this.columnNamingStrategy = config.getColumnNamingStrategy();
        this.jdbcUrl = config.getJdbcUrl();
        this.jdbcUsername = config.getJdbcUsername();
        this.jdbcPassword = config.getJdbcPassword();
        this.jdbcDriverClass = config.getJdbcDriverClass();
        // Debezium normalizes timestamps from source data
        // So we get a timezone with UTC
        this.timeZone = TimeZone.getTimeZone(ZoneOffset.UTC);
        this.calendar = Calendar.getInstance(timeZone);
        this.identifierRule = getIdentifierRule();
        this.columnFilter = getColumnFilter();
    }

    /**
     * Get identifier rule by using JDBC interface getIdentifierQuoteString
     *
     * @return identifier rule {@link IdentifierRule}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected IdentifierRule getIdentifierRule() {
        try (Connection connection = getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            return new IdentifierRule(".", databaseMetaData.getIdentifierQuoteString());
        } catch (SQLException e) {
            throw new ConnectException("failed to get identifier quote string", e);
        }
    }

    /**
     * Build insert sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return insert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public String buildInsertStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition) {
        List<ColumnDefinition> pkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getPrimaryKeyFieldNames());
        List<ColumnDefinition> nonPkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getNonPrimaryKeyFieldNames());
        return getInsertSql(tableDefinition.getTableId(), pkColumnDefinitions, nonPkColumnDefinitions);
    }

    /**
     * Build delete sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return delete sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public String buildDeleteStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition) {
        List<ColumnDefinition> pkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getPrimaryKeyFieldNames());
        List<ColumnDefinition> nonPkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getNonPrimaryKeyFieldNames());
        return getDeleteSql(tableDefinition.getTableId(), pkColumnDefinitions, nonPkColumnDefinitions);
    }

    /**
     * Build update sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return update sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public String buildUpdateStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition) {
        List<ColumnDefinition> pkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getPrimaryKeyFieldNames());
        List<ColumnDefinition> nonPkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getNonPrimaryKeyFieldNames());
        return getUpdateSql(tableDefinition.getTableId(), pkColumnDefinitions, nonPkColumnDefinitions);
    }

    /**
     * Build upsert sql statement
     *
     * @param fieldsMetaData  field metadata from sink record including pk and non-pk {@link FieldsMetaData}
     * @param tableDefinition table definition {@link TableDefinition}
     * @return upsert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public String buildUpsertStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition) {
        List<ColumnDefinition> pkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getPrimaryKeyFieldNames());
        List<ColumnDefinition> nonPkColumnDefinitions = getColumnDefinitions(tableDefinition, fieldsMetaData.getNonPrimaryKeyFieldNames());
        return getUpsertSql(tableDefinition.getTableId(), pkColumnDefinitions, nonPkColumnDefinitions);
    }

    private List<ColumnDefinition> getColumnDefinitions(TableDefinition tableDefinition, List<String> columnNames) {
        TableId tableId = tableDefinition.getTableId();
        List<ColumnDefinition> columnDefinitions = new ArrayList<>(tableDefinition.getColumnDefinitions().size());
        for (String fieldName : columnNames) {
            String columnName = columnNamingStrategy.resolveColumnName(fieldName);
            ColumnDefinition columnDefinition = tableDefinition.getColumnDefinition(columnName);
            if (columnDefinition == null) {
                throw new IllegalArgumentException(String.format("resolved column '%s' from " +
                    "record column '%s',table '%s' not exists!", columnName, fieldName, tableId));
            }
            Optional<ColumnDefinition> optional = columnFilter.apply(columnDefinition);
            if (optional.isEmpty()) {
                continue;
            }
            columnDefinitions.add(optional.get());
        }
        return columnDefinitions;
    }

    /**
     * Create expression builder with specified identifier rule
     *
     * @return expression builder {@link ExpressionBuilder}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected ExpressionBuilder createExpressionBuilder() {
        return new ExpressionBuilder(identifierRule);
    }

    /**
     * Get insert sql statement
     * Almost all the database dialects have the same insert statement sql syntax
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return insert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected String getInsertSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions) {
        final ExpressionBuilder expressionBuilder = createExpressionBuilder();
        expressionBuilder.append("insert into ")
            .append(tableId)
            .append("(")
            .listBuilder()
            .delimitedBy(",")
            .of(pkColumnDefinitions, nonPkColumnDefinitions);
        expressionBuilder.append(")")
            .append(" values(")
            .appendMulti(",", "?", pkColumnDefinitions.size() + nonPkColumnDefinitions.size())
            .append(")");
        return expressionBuilder.toString();
    }

    /**
     * Get upsert sql statement
     * Each database dialect has its own upsert sql statement syntax,
     * If there is a primary key conflict error when executing the insert statement,
     * upsert statement is used to try again.
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return upsert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected abstract String getUpsertSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions);

    /**
     * Get update sql statement
     * Almost all the database dialects have the same update statement sql syntax
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return insert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public String getUpdateSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions) {
        final ExpressionBuilder expressionBuilder = createExpressionBuilder();
        expressionBuilder.append("update ")
            .append(tableId)
            .append(" set ")
            .listBuilder()
            .delimitedBy(",")
            .transformedBy(this::transformToAssignmentExpression)
            .of(nonPkColumnDefinitions);
        expressionBuilder.append(" where ")
            .listBuilder()
            .delimitedBy(" and ")
            .transformedBy(this::transformToAssignmentExpression)
            .of(pkColumnDefinitions);
        return expressionBuilder.toString();
    }

    /**
     * Get delete sql statement
     * Almost all the database dialects have the same delete statement sql syntax
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return insert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public String getDeleteSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions) {
        final ExpressionBuilder expressionBuilder = createExpressionBuilder();
        expressionBuilder.append("delete from ")
            .append(tableId);
        expressionBuilder.append(" where ")
            .listBuilder()
            .delimitedBy(" and ")
            .transformedBy(this::transformToAssignmentExpression)
            .of(pkColumnDefinitions.size() > 0 ? pkColumnDefinitions : nonPkColumnDefinitions);
        return expressionBuilder.toString();
    }

    /**
     * Transform column expression to column assignment expression with binding parameter
     * like "col1" ---> "col1" = ?
     *
     * @param builder          expression builder {@link ExpressionBuilder}
     * @param columnDefinition column definition
     * @return insert sql statement
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected void transformToAssignmentExpression(ExpressionBuilder builder, Object columnDefinition) {
        builder.append(columnDefinition);
        builder.append(" = ?");
    }

    /**
     * Create prepared statement
     *
     * @param sql        sql statement
     * @param connection connection
     * @return prepared statement {@link PreparedStatement}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public PreparedStatement createPreparedStatement(String sql, Connection connection) throws SQLException {
        return connection.prepareStatement(sql);
    }

    /**
     * Create prepared statement binder  {@link PreparedStatementBinder}
     *
     * @param fieldsMetaData  fields metadata
     * @param tableDefinition table definition
     * @return prepared statement {@link PreparedStatementBinder}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public StatementBinder createStatementBinder(
        FieldsMetaData fieldsMetaData,
        TableDefinition tableDefinition
    ) {
        return new PreparedStatementBinder(
            fieldsMetaData,
            tableDefinition,
            columnFilter,
            this
        );
    }

    /**
     * Judge if one table exists
     *
     * @param tableId table identifier {@link TableId}
     * @return if the table exists
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public boolean tableExists(Connection connection, TableId tableId) throws SQLException {
        final DatabaseMetaData databaseMetaData = connection.getMetaData();
        final String catalog = tableId.getCatalog();
        final String schema = tableId.getSchema();
        final String table = tableId.getTable();
        try (ResultSet resultSet = databaseMetaData.getTables(catalog, schema, table, new String[] {"TABLE"})) {
            while (resultSet.next()) {
                if (Objects.equals(catalog, resultSet.getObject(1))
                    && Objects.equals(schema, resultSet.getObject(2))
                    && Objects.equals(table, resultSet.getObject(3))) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Read one table definition
     *
     * @return table definition {@link TableDefinition}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public TableDefinition readTable(Connection connection, TableId tableId) throws SQLException {
        final String catalog = tableId.getCatalog();
        final String schema = tableId.getSchema();
        final String table = tableId.getTable();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
        try (ResultSet rs = databaseMetaData.getColumns(catalog, schema, table, null)) {
            while (rs.next()) {
                final String name = rs.getString(4);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int length = rs.getInt(7);
                final int position = rs.getInt(17);
                final Optional<Integer> scale = rs.getObject(9) != null ?
                    Optional.of(rs.getInt(9)) : Optional.empty();
                String autogenerated = null;
                try {
                    autogenerated = rs.getString(24);
                } catch (SQLException e) {
                    // ignore, some drivers don't have this index - e.g. Postgres
                }
                final ColumnDefinition columnDefinition = ColumnDefinition.builder()
                    .name(name)
                    .jdbcType(jdbcType)
                    .scale(scale)
                    .length(length)
                    .typeName(typeName)
                    .isGenerated("YES".equalsIgnoreCase(autogenerated))
                    .position(position)
                    .build();
                columnDefinitions.add(position - 1, columnDefinition);
            }
        }
        final List<String> pkColumnNames = new ArrayList<>();
        try (ResultSet rs = databaseMetaData.getPrimaryKeys(catalog, schema, table)) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                int columnIndex = rs.getInt(5);
                pkColumnNames.add(columnIndex - 1, columnName);
            }
        }
        return TableDefinition.builder()
            .tableId(tableId)
            .columnDefinitions(columnDefinitions)
            .primaryKeyColumNames(pkColumnNames)
            .build();
    }

    /**
     * Get Connection
     *
     * @return connection {@link Connection}
     * @throws SQLException if there is an error using JDBC interface
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public Connection getConnection() throws SQLException {
        try {
            if (!StringUtil.isEmpty(jdbcDriverClass)) {
                Class.forName(jdbcDriverClass);
            }
            Properties properties = getJdbcProperties();
            properties.put("user", jdbcUsername);
            properties.put("password", jdbcPassword);
            return DriverManager.getConnection(jdbcUrl, properties);
        } catch (ClassNotFoundException e) {
            throw new ConnectException("failed to load jdbc driver class \"" + jdbcDriverClass + "\"", e);
        }
    }

    /**
     * Get Column bind filter,filter out the generated columns
     *
     * @return column filter {@link ColumnFilter}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected ColumnFilter getColumnFilter() {
        return columnDefinition -> columnDefinition.isGenerated() ? Optional.empty() : Optional.of(columnDefinition);
    }

    /**
     * Resolve sql state  {@link SQLState}
     *
     * @param sqlState sql state
     * @return sql state {@link SQLState}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    @Override
    public SQLState resolveSQLState(String sqlState) {
        return SQLState.ERR_UNKNOWN;
    }

    /**
     * Get Jdbc Properties, some database connections must add extract properties
     *
     * @return jdbc Properties {@link Properties}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    protected Properties getJdbcProperties() {
        return new Properties();
    }

    /**
     * Bind field
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
    public void bindField(
        PreparedStatement statement,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition,
        int index
    ) throws SQLException {
        if (value == null) {
            statement.setObject(index, null);
            return;
        }
        if (bindLogical(statement, value, schema, columnDefinition, index)) {
            return;
        }
        if (bindDebezium(statement, value, schema, columnDefinition, index)) {
            return;
        }
        if (bindPrimitive(statement, value, schema, columnDefinition, index)) {
            return;
        }
        statement.setObject(index, value);
    }

    /**
     * Bind logical field
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
    protected boolean bindLogical(
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
            case Decimal.LOGICAL_NAME: {
                statement.setBigDecimal(index, (BigDecimal) value);
                return true;
            }
            case Date.LOGICAL_NAME: {
                statement.setDate(index, new java.sql.Date(((java.util.Date) value).getTime()),
                    Calendar.getInstance(TimeZone.getTimeZone("GMT")));
                return true;
            }
            case Timestamp.LOGICAL_NAME: {
                statement.setTimestamp(index, new java.sql.Timestamp(((java.util.Date) value).getTime()),
                    Calendar.getInstance(TimeZone.getTimeZone("GMT")));
                return true;
            }
            case Time.LOGICAL_NAME: {
                statement.setTime(index, new java.sql.Time(((java.util.Date) value).getTime()),
                    Calendar.getInstance(TimeZone.getTimeZone("GMT")));
                return true;
            }
            default:
                return false;
        }
    }

    /**
     * Bind debezium field
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
            case "io.debezium.time.Timestamp": {
                statement.setTimestamp(index, new java.sql.Timestamp((Long) value), calendar);
                return true;
            }
            case "io.debezium.time.NanoTimestamp": {
                statement.setTimestamp(index, DateTimeUtil.toTimestamp((Long) value), calendar);
                return true;
            }
            case "io.debezium.time.MicroTimestamp": {
                statement.setTimestamp(
                    index,
                    DateTimeUtil.toTimestamp((TimeUnit.MICROSECONDS.toNanos((Long) value))),
                    calendar
                );
                return true;
            }
            case "io.debezium.time.ZonedTimestamp": {
                statement.setTimestamp(index, DateTimeUtil.toTimestampFromIsoDateTime((String) value, timeZone));
                return true;
            }
            case "io.debezium.time.Date": {
                Integer days = (Integer) value;
                statement.setDate(index, new java.sql.Date(3600*24*1000L * days), calendar);
                return true;
            }
            default:
                return false;
        }
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
            case FLOAT32: {
                statement.setFloat(index, (Float) value);
                return true;
            }
            case FLOAT64: {
                statement.setDouble(index, (Double) value);
                return true;
            }
            case BOOLEAN: {
                statement.setBoolean(index, (Boolean) value);
                return true;
            }
            case INT32: {
                statement.setInt(index, (Integer) value);
                return true;
            }
            case INT16: {
                statement.setShort(index, (Short) value);
                return true;
            }
            case INT8: {
                statement.setByte(index, (Byte) value);
                return true;
            }
            case STRING: {
                statement.setString(index, (String) value);
                return true;
            }
            case BYTES: {
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                statement.setBytes(index, bytes);
                return true;
            }
            default:
                return false;
        }
    }

}
