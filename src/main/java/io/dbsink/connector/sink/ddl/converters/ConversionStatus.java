package io.dbsink.connector.sink.ddl.converters;

/**
 * DDL Conversion Status
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public enum ConversionStatus {
    /**
     * Succeeded to convert sql statement from source database to target database
     */
    SUCCEEDED,
    /**
     * Failed to convert sql statement from source database to target database
     */
    FAILED
}
