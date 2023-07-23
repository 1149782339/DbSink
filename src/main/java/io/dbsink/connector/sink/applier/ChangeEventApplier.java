/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.dialect.DatabaseDialect;
import io.dbsink.connector.sink.dialect.DatabaseDialects;
import io.dbsink.connector.sink.dialect.DatabaseType;
import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.Operation;
import io.dbsink.connector.sink.event.SchemaChangeEvent;
import io.dbsink.connector.sink.event.TransactionEvent;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.naming.ColumnNamingStrategy;
import io.dbsink.connector.sink.naming.TableNamingStrategy;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableId;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;


/**
 * ChangeEvent Applier parse sink records {@link SinkRecord} and resolve as transaction events {@link TransactionEvent}
 * schema change events {@link SchemaChangeEvent},data change events {@link DataChangeEvent}, and applies them through
 * its internal applier
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public class ChangeEventApplier implements Applier<Collection<SinkRecord>>{

    private final static String SCHEMA_CHANGE_EVENT = "SchemaChangeValue";

    private final static String TRANSACTION_EVENT = "TransactionMetadataValue";

    private final static String HEARTBEAT_EVENT = "Heartbeat";

    private final static Logger LOGGER = LoggerFactory.getLogger(ChangeEventApplier.class);

    private final TableNamingStrategy tableNamingStrategy;

    private final ColumnNamingStrategy columnNamingStrategy;

    private final Applier applier;

    private final boolean applyDDLEnabled;


    public ChangeEventApplier(Applier internalApplier, ConnectorConfig config) {
        this.applier = internalApplier;
        this.tableNamingStrategy = config.getTableNamingStrategy();
        this.columnNamingStrategy = config.getColumnNamingStrategy();
        this.applyDDLEnabled = config.getApplierDDLEnabled();
    }

    @Override
    public void prepare(TaskContext taskContext) {
        this.applier.prepare(taskContext);
    }

    @Override
    public void apply(Collection<SinkRecord> records) throws ApplierException {
        if (records.size() == 0) {
            return;
        }
        List<ChangeEvent> events = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            final Struct value = (Struct) record.value();
            final String topic = record.topic();
            final Integer partition = record.kafkaPartition();
            final Long offset = record.kafkaOffset();
            Schema schema = record.valueSchema();
            ChangeEvent event;
            // data change event
            if (schema.name().endsWith(SCHEMA_CHANGE_EVENT)) {
                // schema change event
                if (!applyDDLEnabled) {
                    LOGGER.info("schema change event is omitted");
                    continue;
                }
                DatabaseType databaseType = DatabaseType.valueOf(value.getStruct("source")
                    .getString("connector").toUpperCase(Locale.ROOT));
                String ddl = value.getString("ddl");
                TableId tableId = getTableIdentifier(value);
                event = SchemaChangeEvent.builder()
                    .ddl(ddl)
                    .tableId(tableId)
                    .offset(offset)
                    .partition(partition)
                    .topic(topic)
                    .databaseType(databaseType)
                    .build();
            } else if (schema.name().endsWith(TRANSACTION_EVENT)) {
                // transaction event
                String txId = value.getString("id");
                String status = value.getString("status");
                event = TransactionEvent.build()
                    .transactionId(txId)
                    .status(TransactionEvent.Status.valueOf(status))
                    .offset(offset)
                    .partition(partition)
                    .topic(topic)
                    .build();
            } else if (schema.name().endsWith(HEARTBEAT_EVENT)) {
                // ignore heartbeat event
                LOGGER.debug("ignore heartbeat event");
                continue;
            } else {
                TableId tableId = getTableIdentifier(value);
                Operation operation = Operation.fromString(value.getString("op"));
                Map<String, Object> beforeValue = getValues(value.getStruct("before"));
                Map<String, Object> afterValue = getValues(value.getStruct("after"));
                FieldsMetaData fieldsMetaData = FieldsMetaData.extractFieldsMetaData(record, columnNamingStrategy);
                DatabaseType databaseType = DatabaseType.valueOf(value.getStruct("source")
                    .getString("connector").toUpperCase(Locale.ROOT));
                event = DataChangeEvent.builder()
                    .offset(offset)
                    .partition(partition)
                    .topic(topic)
                    .tableId(tableId)
                    .operation(operation)
                    .afterValues(afterValue)
                    .beforeValues(beforeValue)
                    .transactionId(getTransactionId(value))
                    .fieldsMetaData(fieldsMetaData)
                    .databaseType(databaseType)
                    .build();
            }
            events.add(event);
        }
        applier.apply(events);
    }

    private TableId getTableIdentifier(Struct value) {
        Struct struct = value.getStruct("source");
        String catalog = struct.schema().field("db") != null ? struct.getString("db") : null;
        String schema = struct.schema().field("schema") != null ? struct.getString("schema") : null;
        String table = struct.schema().field("table") != null ? struct.getString("table") : null;
        return tableNamingStrategy.resolveTableId(new TableId(catalog, schema, table));
    }

    private String getTransactionId(Struct value) {
        Field field = value.schema().field("transaction");
        if (field == null) {
            return null;
        }
        Struct transactionMetaData = value.getStruct("transaction");
        if (transactionMetaData == null) {
            return null;
        }
        return transactionMetaData.getString("id");
    }

    private Map<String, Object> getValues(Struct value) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (value == null) {
            return map;
        }
        for (Field field : value.schema().fields()) {
            String columnName = columnNamingStrategy.resolveColumnName(field.name());
            map.put(columnName, value.get(field));
        }
        return map;
    }

    @Override
    public void release() {
        applier.release();
    }
}
