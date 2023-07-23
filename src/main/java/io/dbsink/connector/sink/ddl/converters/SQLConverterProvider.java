package io.dbsink.connector.sink.ddl.converters;

import io.dbsink.connector.sink.dialect.DatabaseType;

/**
 * SQL converter provider
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public interface SQLConverterProvider {
    /**
     * Source database of the SQL converter
     *
     * @return database type {@link DatabaseType}
     * @author Wang Wei
     * @time: 2023-07-22
     */
    DatabaseType sourceDatabase();

    /**
     * Target database of the SQL converter
     *
     * @return database type {@link DatabaseType}
     * @author Wang Wei
     * @time: 2023-07-22
     */
    DatabaseType targetDatabase();

    /**
     * Create SQL converter
     *
     * @param configuration {@link ConversionConfiguration}
     * @return SQL converter {@link SQLConverter}
     * @author Wang Wei
     * @time: 2023-07-22
     */
    SQLConverter create(ConversionConfiguration configuration);
}
