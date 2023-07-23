package io.dbsink.connector.sink.ddl.converters;

import io.dbsink.connector.sink.dialect.DatabaseType;
import io.dbsink.connector.sink.util.Pair;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * SQL converters, loads all SQL converter {@link SQLConverter} and
 * provides an interface to create a converter
 *
 * @author Wang Wei
 * @time: 2023-07-22
 */
public class SQLConverters {

    private static Map<Pair<DatabaseType, DatabaseType>, SQLConverterProvider> SQL_CONVERTER_REGISTRY = new HashMap<>();

    static {
        loadSQLConverters();
    }

    private static void loadSQLConverters() {
        for (SQLConverterProvider converterProvider : ServiceLoader.load(SQLConverterProvider.class)) {
            DatabaseType sourceDatabase = converterProvider.sourceDatabase();
            DatabaseType targetDatabase = converterProvider.targetDatabase();
            SQL_CONVERTER_REGISTRY.put(Pair.of(sourceDatabase, targetDatabase), converterProvider);
        }
    }

    /**
     * Create SQL converter
     *
     * @param sourceDatabase source database {@link DatabaseType}
     * @param targetDatabase target database {@link DatabaseType}
     * @param configuration  conversion configuration {@link ConversionConfiguration}
     * @return SQL converter {@link SQLConverter}
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public static SQLConverter create(
        DatabaseType sourceDatabase,
        DatabaseType targetDatabase,
        ConversionConfiguration configuration
    ) {
        SQLConverterProvider converterProvider = SQL_CONVERTER_REGISTRY.get(Pair.of(sourceDatabase, targetDatabase));
        if (converterProvider == null) {
            throw new ConnectException("sql conversion from \"" + sourceDatabase + "\" to \""
                + targetDatabase + "\" is not supported!");
        }
        return converterProvider.create(configuration);
    }
}
