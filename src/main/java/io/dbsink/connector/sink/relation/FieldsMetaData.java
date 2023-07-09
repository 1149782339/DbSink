/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.relation;

import io.dbsink.connector.sink.naming.ColumnNamingStrategy;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

/**
 * Fields MetaData
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public class FieldsMetaData {
    /**
     * primary key field names
     */
    private final List<String> primaryKeyFieldNames;
    /**
     * primary key field names
     */
    private final List<String> nonPrimaryKeyFieldNames;

    public Map<String, Schema> getFieldSchemas() {
        return fieldSchemas;
    }

    /**
     * field schemas
     */
    private final Map<String, Schema> fieldSchemas;

    public FieldsMetaData(List<String> primaryKeyColumnNames, List<String> nonPrimaryKeyColumnNames, Map<String, Schema> fieldSchemas) {
        this.primaryKeyFieldNames = primaryKeyColumnNames;
        this.nonPrimaryKeyFieldNames = nonPrimaryKeyColumnNames;
        this.fieldSchemas = fieldSchemas;
    }

    public Schema getFieldSchema(String fieldName) {
        return fieldSchemas.get(fieldName);
    }

    public List<String> getPrimaryKeyFieldNames() {
        return primaryKeyFieldNames;
    }

    public List<String> getNonPrimaryKeyFieldNames() {
        return nonPrimaryKeyFieldNames;
    }

    private static Set<String> getFieldNames(Schema schema, ColumnNamingStrategy columnNamingStrategy) {
        final Set<String> primaryKeyFieldNames = new LinkedHashSet<>();
        if (schema == null) {
            return primaryKeyFieldNames;
        }
        for (Field field : schema.fields()) {
            primaryKeyFieldNames.add(columnNamingStrategy.resolveColumnName(field.name()));
        }
        return primaryKeyFieldNames;
    }

    /**
     * Extract fields metadata from sink record
     *
     * @param sinkRecord           {@link SinkRecord}
     * @param columnNamingStrategy {@link ColumnNamingStrategy}
     * @author Wang Wei
     * @time: 2023-06-09
     */
    public static FieldsMetaData extractFieldsMetaData(SinkRecord sinkRecord, ColumnNamingStrategy columnNamingStrategy) {
        final Struct value = (Struct) sinkRecord.value();
        final Struct struct = value.getStruct("after") != null ?
            value.getStruct("after") : value.getStruct("before");
        final Map<String, Schema> fieldSchemas = getFieldSchema(struct, columnNamingStrategy);
        final Set<String> primaryKeyFieldNamesSet = getFieldNames(sinkRecord.keySchema(), columnNamingStrategy);
        final Set<String> fieldNamesSet = getFieldNames(struct.schema(), columnNamingStrategy);

        final List<String> primaryKeyFieldNames = new ArrayList<>();
        final List<String> nonPrimaryKeyFieldNames = new ArrayList<>();
        for (String fieldName : fieldNamesSet) {
            if (primaryKeyFieldNamesSet.contains(fieldName)) {
                primaryKeyFieldNames.add(fieldName);
            } else {
                nonPrimaryKeyFieldNames.add(fieldName);
            }
        }
        return new FieldsMetaData(primaryKeyFieldNames, nonPrimaryKeyFieldNames, fieldSchemas);
    }

    /**
     * Get fields schema from sink record
     *
     * @param value                struct value           {@link SinkRecord}
     * @param columnNamingStrategy column naming strategy {@link ColumnNamingStrategy}
     * @author Wang Wei
     * @time: 2023-06-09
     */
    private static Map<String, Schema> getFieldSchema(Struct value, ColumnNamingStrategy columnNamingStrategy) {
        Map<String, Schema> map = new LinkedHashMap<>();
        for (Field field : value.schema().fields()) {
            String columnName = columnNamingStrategy.resolveColumnName(field.name());
            map.put(columnName, field.schema());
        }
        return map;
    }
}
