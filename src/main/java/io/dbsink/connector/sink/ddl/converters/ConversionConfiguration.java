package io.dbsink.connector.sink.ddl.converters;

import io.dbsink.connector.sink.naming.ColumnNamingStrategy;
import io.dbsink.connector.sink.naming.TableNamingStrategy;

/**
 * Conversion configuration used by sql converter
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class ConversionConfiguration {
    private ColumnNamingStrategy columnNamingStrategy;
    private TableNamingStrategy tableNamingStrategy;

    public ConversionConfiguration(TableNamingStrategy tableNamingStrategy, ColumnNamingStrategy columnNamingStrategy) {
        this.tableNamingStrategy = tableNamingStrategy;
        this.columnNamingStrategy = columnNamingStrategy;
    }

    public ColumnNamingStrategy getColumnNamingStrategy() {
        return columnNamingStrategy;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }
}
