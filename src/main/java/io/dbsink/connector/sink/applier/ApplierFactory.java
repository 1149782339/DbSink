package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.context.ConnectorContext;

public class ApplierFactory {

    public static Applier createTableApplier(ConnectorContext context, ConnectorConfig config) {
        if ("MongoDialect".equals(config.getDatabaseDialectName())) {
            return new MongoApplier(context, config);
        } else {
            return new JdbcApplier(context, config);
        }
    }
}
