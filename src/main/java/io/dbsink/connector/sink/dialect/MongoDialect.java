package io.dbsink.connector.sink.dialect;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.naming.ColumnNamingStrategy;
import io.dbsink.connector.sink.relation.ColumnDefinition;
import org.apache.kafka.connect.data.Schema;
import org.bson.Document;


public class MongoDialect {
    /**
     * Column naming strategy
     */
    protected final ColumnNamingStrategy columnNamingStrategy;

    public MongoDialect(ConnectorConfig config) {
        this.columnNamingStrategy = config.getColumnNamingStrategy();
    }

    public void bindField(
        Document document,
        Object value,
        Schema schema,
        ColumnDefinition columnDefinition
    ) {

    }
}
