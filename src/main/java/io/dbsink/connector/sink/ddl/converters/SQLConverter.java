package io.dbsink.connector.sink.ddl.converters;

/**
 * SQL converter
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public interface SQLConverter {

    /**
     * Convert sql statement from source database to target database
     *
     * @param statement sql statement
     * @return SQL conversion result {@link ConversionResult}
     * @author Wang Wei
     * @time: 2023-07-22
     */
    ConversionResult convert(String statement);
}
