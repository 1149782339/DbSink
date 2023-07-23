package io.dbsink.connector.sink.util;

import io.dbsink.connector.sink.relation.TableId;

import java.util.HashMap;
import java.util.Map;

/**
 * The mapping relationships between the source database and
 * the target database for tables and columns
 *
 * @author: Wang Wei
 * @time: 2023-07-16
 */
public class RelationMappings {
    private final Map<TableId, Map<String, String>> columnMappings;

    private final Map<TableId, TableId> tableMappings;

    public RelationMappings() {
        this.columnMappings = new HashMap<>();
        this.tableMappings = new HashMap<>();
    }

    public void addColumMapping(TableId tableId, String sourceColumnName, String targetColumnName) {
        Map<String,String> columnMapping = columnMappings.get(tableId);
        if (columnMapping == null) {
            columnMapping = new HashMap<>();
            columnMappings.put(tableId, columnMapping);
        }
        columnMapping.put(sourceColumnName, targetColumnName);
    }

    public void addTableMapping(TableId sourceTableId, TableId targetTableId) {
        tableMappings.put(sourceTableId, targetTableId);
    }

    public TableId convert(TableId tableId) {
        TableId target = tableMappings.get(tableId);
        return target;
    }

    public String convert(TableId tableId, String columName) {
        Map<String,String> columnMapping = columnMappings.get(tableId);
        if (columnMapping == null) {
            return null;
        }
        String targetColumnName = columnMapping.get(columName);
        if (targetColumnName == null) {
            return null;
        }
        return targetColumnName;
    }
}
