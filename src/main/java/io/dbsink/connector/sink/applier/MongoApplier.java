package io.dbsink.connector.sink.applier;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.annotation.ThreadSafe;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.Operation;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableId;
import io.dbsink.connector.sink.util.StringUtil;
import io.dbsink.connector.sink.util.TimeTracker;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Mongo DB Applier
 *
 * @author: Wang Wei
 * @time: 2023-07-16
 */
@ThreadSafe
public class MongoApplier implements Applier<Collection<ChangeEvent>> {

    private final static Logger LOGGER = LoggerFactory.getLogger(MongoApplier.class);

    private final int MAX_UPSERT_INTERVAL = 120000;

    private final static String APPLICATION_NAME = "MongoDBApplier";

    private MongoClient mongoClient;

    private TableId tableId = null;

    private MongoCollection<Document> collection;

    private final List<DataChangeEvent> events;

    private final ConnectorConfig config;

    private final ConnectorContext context;

    private final TimeTracker timeTracker;

    private boolean upsertMode = false;

    public MongoApplier(ConnectorContext context, ConnectorConfig config) {
        this.config = config;
        this.events = new ArrayList<>();
        this.context = context;
        this.timeTracker = new TimeTracker();
    }

    private MongoClient getConnection() {
        String connectionString = config.getConnectionUrl();
        ConnectionString connString = new ConnectionString(connectionString);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
            .applyConnectionString(connString)
            .retryWrites(true)
            .applicationName(APPLICATION_NAME);
        MongoClientSettings settings = builder.build();
        MongoClient client = MongoClients.create(settings);
        return client;
    }

    @Override
    public void prepare(TaskContext taskContext) {
        this.mongoClient = getConnection();
    }

    @Override
    public void release() {
        if (this.mongoClient != null) {
            this.mongoClient.close();
        }
    }

    private Operation getDataChangeEventOperation(DataChangeEvent event) {
        if (upsertMode && (event.getOperation().equals(Operation.CREATE) || event.getOperation().equals(Operation.READ))) {
            return Operation.UPSERT;
        } else {
            return event.getOperation();
        }
    }

    private void handleDataChangeEvent(DataChangeEvent event) {
        if (tableId == null) {
            collection = getCollection(event.getTableId());
            tableId = event.getTableId();
        }
        if (!event.getTableId().equals(tableId)) {
            flush();
            collection = getCollection(event.getTableId());
            tableId = event.getTableId();
        }
        events.add(event);
    }

    private MongoCollection<Document> getCollection(TableId tableId) {
        String database;
        String table;
        if (!StringUtil.isEmpty(tableId.getCatalog())) {
            database = tableId.getCatalog();
        } else if (!StringUtil.isEmpty(tableId.getSchema())) {
            database = tableId.getSchema();
        } else {
            throw new IllegalArgumentException("invalid table id \"" + tableId + "\"");
        }
        if (!StringUtil.isEmpty(tableId.getTable())) {
            table = tableId.getTable();
        } else {
            throw new IllegalArgumentException("invalid table id \"" + tableId + "\"");
        }
        return mongoClient.getDatabase(database).getCollection(table);
    }

    private Object toMongoDbData(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        if (schema.name() != null) {
            switch (schema.name()) {
                case Decimal.LOGICAL_NAME: {
                    return ((BigDecimal) value).doubleValue();
                }
                case Date.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                case Time.LOGICAL_NAME: {
                    return value;
                }
                case "io.debezium.time.Timestamp": {
                    return new BSONTimestamp((int) TimeUnit.MILLISECONDS.toSeconds(((Long) value)), 0);
                }
                case "io.debezium.time.NanoTimestamp": {
                    return new BSONTimestamp((int) TimeUnit.NANOSECONDS.toSeconds(((Long) value)), 0);
                }
                case "io.debezium.time.MicroTimestamp": {
                    return new BSONTimestamp((int) TimeUnit.MICROSECONDS.toSeconds(((Long) value)), 0);
                }
                case "io.debezium.time.ZonedTimestamp": {
                    int epochSecond = (int) LocalDateTime.parse((String) value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                        .atZone(ZoneOffset.UTC)
                        .toLocalDateTime()
                        .toEpochSecond(ZoneOffset.UTC);
                    return new BSONTimestamp(epochSecond, 0);
                }
                case "io.debezium.time.Date": {
                    Integer days = (Integer) value;
                    return new java.util.Date(3600 * 24 * 1000L * days);
                }
            }
        }
        switch (schema.type()) {
            case FLOAT32: {
                return Double.valueOf((Float) value);
            }
            case BYTES: {
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                return new Binary(bytes);
            }
        }
        return value;
    }

    private Object getPrimaryKeyValue(Map<String, Object> values, FieldsMetaData fieldsMetaData) {
        final List<String> primaryKeyFieldNames = fieldsMetaData.getPrimaryKeyFieldNames();
        final int size = fieldsMetaData.getPrimaryKeyFieldNames().size();
        if (size == 0) {
            return null;
        } else if (fieldsMetaData.getPrimaryKeyFieldNames().size() == 1) {
            String fieldName = fieldsMetaData.getPrimaryKeyFieldNames().get(0);
            return toMongoDbData(values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName));
        } else {
            StringBuilder sb = new StringBuilder(32);
            for (int i = 0; i < size; i++) {
                String fieldName = primaryKeyFieldNames.get(i);
                sb.append(StringUtil.toString(values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName)));
                if (i < size - 1) {
                    sb.append("_");
                }
            }
            return sb.toString();
        }
    }

    private Document update(Map<String, Object> values, FieldsMetaData fieldsMetaData) {
        final Document document = new Document();
        for (String fieldName : fieldsMetaData.getPrimaryKeyFieldNames()) {
            document.append(fieldName, toMongoDbData(values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName)));
        }
        for (String fieldName : fieldsMetaData.getNonPrimaryKeyFieldNames()) {
            document.append(fieldName, toMongoDbData(values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName)));
        }
        return document;
    }

    private Document create(Map<String, Object> values, FieldsMetaData fieldsMetaData, boolean onlyPrimaryKey) {
        final Document document = new Document();
        final Object getPrimaryKeyValue = getPrimaryKeyValue(values, fieldsMetaData);
        if (getPrimaryKeyValue != null) {
            document.append("_id", getPrimaryKeyValue);
        }
        if (getPrimaryKeyValue != null && onlyPrimaryKey) {
            return document;
        }
        for (String fieldName : fieldsMetaData.getPrimaryKeyFieldNames()) {
            document.append(fieldName, toMongoDbData(values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName)));
        }
        for (String fieldName : fieldsMetaData.getNonPrimaryKeyFieldNames()) {
            document.append(fieldName, toMongoDbData(values.get(fieldName), fieldsMetaData.getFieldSchema(fieldName)));
        }
        return document;
    }

    private void flush() {
        List<WriteModel<Document>> writeModels = new ArrayList<>();
        for (DataChangeEvent event : events) {
            final FieldsMetaData fieldsMetaData = event.getFieldsMetaData();
            final Map<String, Object> beforeValues = event.getBeforeValues();
            final Map<String, Object> afterValues = event.getAfterValues();
            final Operation operation = getDataChangeEventOperation(event);
            if (operation == Operation.UPDATE) {
                Document query = create(beforeValues, fieldsMetaData, true);
                Document update = new Document("$set", update(afterValues, fieldsMetaData));
                UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(query, update);
                writeModels.add(updateOneModel);
            } else if (operation == Operation.READ || operation == Operation.CREATE) {
                Document data = create(afterValues, fieldsMetaData, false);
                InsertOneModel<Document> insertOneModel = new InsertOneModel<>(data);
                writeModels.add(insertOneModel);
            } else if (operation == Operation.DELETE) {
                Document query = create(beforeValues, fieldsMetaData, true);
                DeleteOneModel<Document> deleteOneModel = new DeleteOneModel<>(query);
                writeModels.add(deleteOneModel);
            } else if (operation == Operation.UPSERT) {
                Document query = create(afterValues, fieldsMetaData, false);
                Document update = new Document("$set", update(afterValues, fieldsMetaData));
                UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(query, update, new UpdateOptions().upsert(true));
                writeModels.add(updateOneModel);
            } else {
                throw new IllegalArgumentException("unexpected operation: " + operation);
            }
        }
        BulkWriteOptions options = new BulkWriteOptions().ordered(true);
        if (writeModels.isEmpty()) {
            LOGGER.debug("no write modules, just return");
            return;
        }
        try (ClientSession session = mongoClient.startSession()) {
            session.startTransaction();
            BulkWriteResult result = collection.bulkWrite(writeModels, options);
            session.commitTransaction();
            LOGGER.debug("matched count: {}, updated count: {}, insert count: {}, delete count: {}", result.getMatchedCount(),
                result.getModifiedCount(), result.getInsertedCount(), result.getDeletedCount());
        }
        events.clear();
    }

    private void doApply(Collection<ChangeEvent> events) {
        try {
            for (ChangeEvent event : events) {
                if (event instanceof DataChangeEvent) {
                    handleDataChangeEvent((DataChangeEvent) event);
                } else {
                    LOGGER.debug("ignore the event({})", event.getClass());
                }
            }
            flush();
            if (upsertMode && timeTracker.isTimeElapsed(MAX_UPSERT_INTERVAL)) {
                upsertMode = false;
                LOGGER.info("Turn off upsert mode");
            }
        } catch (MongoBulkWriteException e) {
            if (e.getMessage() != null && e.getMessage().contains("dup key")) {
                LOGGER.info("Turn on upsert mode");
                upsertMode = true;
            }
            throw e;
        }
    }

    /**
     * apply the change events {@link ChangeEvent} only including data change events {@link DataChangeEvent}
     *
     * @author: Wang Wei
     * @time: 2023-07-16
     */
    @Override
    public synchronized void apply(Collection<ChangeEvent> events) throws ApplierException {
        int retry = 0;
        while (retry < config.getConnectionRetriesMax()) {
            retry++;
            try {
                this.doApply(events);
                return;
            } catch (MongoException e) {
                if (retry == config.getConnectionRetriesMax()) {
                    throw e;
                }
                try {
                    Thread.sleep(config.getConnectionBackoff());
                } catch (InterruptedException exception) {
                    Thread.interrupted();
                    if (!context.isRunning()) {
                        throw new ApplierException(exception);
                    }
                }
            } finally {
                events.clear();
                tableId = null;
            }
        }
    }
}
