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
import org.apache.kafka.connect.data.Schema;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Opengauss dialect, override postgres database dialect method {@link PostgreSqlDialect}
 *
 * @author Wang Wei
 * @time: 2023-06-23
 */
public class OpengaussDialect extends PostgreSqlDialect {
    public OpengaussDialect(ConnectorConfig config) {
        super(config);
    }

    /**
     * Get upsert sql statement.
     * Opengauss upsert sql statement example:
     * Insert into "migration"."test1"("col1","col2") values(?,?) on duplicate key
     * update set "col2"=excluded."col2";
     *
     * @param tableId                table identifier {@link TableId}
     * @param pkColumnDefinitions    primary key column definitions
     * @param nonPkColumnDefinitions non-primary key column definitions
     * @return upsert sql statement
     * @author: Wang Wei
     * @time: 2023-06-23
     */
    @Override
    protected String getUpsertSql(TableId tableId, List<ColumnDefinition> pkColumnDefinitions, List<ColumnDefinition> nonPkColumnDefinitions) {
        final ExpressionBuilder expressionBuilder = createExpressionBuilder();
        final ExpressionBuilder.Transformer<ColumnDefinition> transformer =
            (builder,colDef)-> builder.append(colDef).append("=excluded.").append(colDef);
        expressionBuilder.append("insert into ")
            .append(tableId)
            .append("(")
            .listBuilder()
            .delimitedBy(",")
            .of(pkColumnDefinitions, nonPkColumnDefinitions);
        expressionBuilder.append(")")
            .append(" values(")
            .appendMulti("," , "?", pkColumnDefinitions.size() + nonPkColumnDefinitions.size())
            .append(") ")
            .append("on duplicate key update ");
        if (nonPkColumnDefinitions.size() == 0) {
            expressionBuilder.append("nothing");
        } else {
            expressionBuilder.listBuilder()
                .delimitedBy(",")
                .transformedBy(transformer)
                .of(nonPkColumnDefinitions);
        }
        return expressionBuilder.toString();
    }

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
            case BYTES: {
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                // Opengauss support blob type
                if (ColumTypeName.BLOB.equals(columnDefinition.getTypeName())) {
                    statement.setBlob(index, new ByteArrayInputStream(bytes));
                } else {
                    statement.setBytes(index, bytes);
                }
                return true;
            }
            default:
                return super.bindPrimitive(statement, value, schema, columnDefinition, index);
        }
    }

    /**
     * Column type name
     *
     * @author Wang Wei
     * @time: 2023-07-01
     */
    public static class  ColumTypeName {
        public static final String BLOB = "blob";
    }

    public static class OpengaussDialectProvider implements DatabaseDialectProvider {

        @Override
        public DatabaseDialect create(ConnectorConfig config) {
            return new OpengaussDialect(config);
        }

        @Override
        public String dialectName() {
            return "OpengaussDialect";
        }
    }
}
