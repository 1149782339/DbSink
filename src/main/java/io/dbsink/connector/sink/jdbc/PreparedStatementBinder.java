/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.jdbc;

import io.dbsink.connector.sink.binding.ColumnFilter;
import io.dbsink.connector.sink.dialect.CommonDatabaseDialect;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.relation.ColumnDefinition;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableDefinition;
import org.apache.kafka.connect.data.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * statement binder defines the interface for DML statement for binding parameters
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public class PreparedStatementBinder implements StatementBinder {

    private final TableDefinition tableDefinition;

    private final FieldsMetaData fieldsMetaData;

    private final ColumnFilter columnFilter;

    private final CommonDatabaseDialect databaseDialect;

    /**
     * PreparedStatementBinder.
     *
     * @param fieldsMetaData  SinkRecord fields metadata
     * @param tableDefinition Table definition
     * @param databaseDialect database dialect
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    public PreparedStatementBinder(
        FieldsMetaData fieldsMetaData,
        TableDefinition tableDefinition,
        ColumnFilter columnFilter,
        CommonDatabaseDialect databaseDialect
    ) {
        this.fieldsMetaData = fieldsMetaData;
        this.columnFilter = columnFilter;
        this.tableDefinition = tableDefinition;
        this.databaseDialect = databaseDialect;
    }

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
    public void bindInsertStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException {
        final Map<String, Object> values = event.getAfterValues();
        int index = 1;
        index = bindFields(statement, values, fieldsMetaData.getPrimaryKeyFieldNames(), index);
        bindFields(statement, values, fieldsMetaData.getNonPrimaryKeyFieldNames(), index);
    }

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
    public void bindUpdateStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException {
        final Map<String, Object> beforeValues = event.getBeforeValues();
        final Map<String, Object> afterValues = event.getAfterValues();
        int index = 1;
        index = bindFields(statement, afterValues, fieldsMetaData.getNonPrimaryKeyFieldNames(), index);
        if (fieldsMetaData.getPrimaryKeyFieldNames().size() > 0) {
            bindFields(statement, beforeValues, fieldsMetaData.getPrimaryKeyFieldNames(), index);
        } else {
            bindFields(statement, beforeValues, fieldsMetaData.getNonPrimaryKeyFieldNames(), index);
        }
    }

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
    public void bindDeleteStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException {
        final Map<String, Object> values = event.getBeforeValues();
        if (fieldsMetaData.getPrimaryKeyFieldNames().size() > 0) {
            bindFields(statement, values, fieldsMetaData.getPrimaryKeyFieldNames(), 1);
        } else {
            bindFields(statement, values, fieldsMetaData.getNonPrimaryKeyFieldNames(), 1);
        }
    }

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
    public void bindUpsertStatement(PreparedStatement statement, DataChangeEvent event) throws SQLException {
        final Map<String, Object> values = event.getAfterValues();
        int index = 1;
        index = bindFields(statement, values, fieldsMetaData.getPrimaryKeyFieldNames(), index);
        bindFields(statement, values, fieldsMetaData.getNonPrimaryKeyFieldNames(), index);
    }

    private int bindFields(
        PreparedStatement statement,
        Map<String, Object> values,
        List<String> fields,
        int index
    ) throws SQLException {
        for (String fieldName : fields) {
            ColumnDefinition columnDefinition = tableDefinition.getColumnDefinition(fieldName);
            if (columnDefinition == null) {
                throw new IllegalArgumentException(String.format("resolved column '%", fieldName,
                    tableDefinition.getTableId()));
            }
            Optional<ColumnDefinition> optional = columnFilter.apply(columnDefinition);
            if (optional.isEmpty()) {
                continue;
            }
            bindField(statement, values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName), optional.get(), index++);
        }
        return index;
    }

    private void bindField(
        PreparedStatement statement,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition,
        int index
    ) throws SQLException {
        databaseDialect.bindField(statement, value, schema, columnDefinition, index);
    }
}
