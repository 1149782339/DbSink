package io.dbsink.connector.sink.ddl.converters;

/**
 * Base class of  each SQL converter
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public abstract class CommonSQLConverter implements SQLConverter {
    protected final ConversionConfiguration configuration;

    public CommonSQLConverter(ConversionConfiguration configuration) {
        this.configuration = configuration;
    }
}
