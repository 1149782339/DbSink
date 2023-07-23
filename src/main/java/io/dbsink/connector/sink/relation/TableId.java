/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.relation;

import io.dbsink.connector.sink.sql.ExpressibleObject;
import io.dbsink.connector.sink.util.StringUtil;
import io.dbsink.connector.sink.sql.ExpressionBuilder;
import io.dbsink.connector.sink.sql.IdentifierRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Table identifier
 *
 * @author Wang Wei
 * @time: 2023-06-21
 */
public class TableId implements ExpressibleObject {

    private String catalog;
    private String schema;
    private String table;

    private final String id;

    public TableId(String catalog, String schema, String table) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.id = tableId(catalog, schema, table);
    }
    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public void append(ExpressionBuilder builder) {
        final IdentifierRule rule = builder.getIdentifierRule();
        final String quoteString = rule.getQuoteString();
        final String delimiter = rule.getDelimiter();
        if (table == null) {
            throw new IllegalArgumentException("table can't be empty");
        }
        if (schema != null) {
            builder.append(StringUtil.quote(schema, quoteString));
            builder.append(delimiter);
            builder.append(StringUtil.quote(table, quoteString));
            return;
        }
        if (catalog != null) {
            builder.append(StringUtil.quote(catalog, quoteString));
            builder.append(delimiter);
            builder.append(StringUtil.quote(table, quoteString));
        }
    }

    public TableId toQuoted(char quotingChar) {
        String catalogName = null;
        if (this.catalog != null && !this.catalog.isEmpty()) {
            catalogName = StringUtil.quote(this.catalog, quotingChar);
        }

        String schemaName = null;
        if (this.schema != null && !this.schema.isEmpty()) {
            schemaName = StringUtil.quote(this.schema, quotingChar);
        }

        return new TableId(catalogName, schemaName, StringUtil.quote(this.table, quotingChar));
    }

    private static String tableId(String catalog, String schema, String table) {
        if (catalog == null || catalog.length() == 0) {
            if (schema == null || schema.length() == 0) {
                return table;
            }
            return schema + "." + table;
        }
        if (schema == null || schema.length() == 0) {
            return catalog + "." + table;
        }
        return catalog + "." + schema + "." + table;
    }
    public String identifier() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableId tableId = (TableId) o;
        return Objects.equals(catalog, tableId.catalog) && Objects.equals(schema, tableId.schema) && table.equals(tableId.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, schema, table);
    }

    @Override
    public String toString() {
        List<String> parts = new ArrayList<>(3);
        if (catalog != null) {
            parts.add(catalog);
        }
        if (schema != null) {
            parts.add(schema);
        }
        if (table != null) {
            parts.add(table);
        }
        return String.join(".", parts);
    }

}
