/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.annotation.ThreadSafe;
import io.dbsink.connector.sink.connection.JdbcConnection;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.dialect.DatabaseDialect;
import io.dbsink.connector.sink.dialect.DatabaseDialects;
import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.Operation;
import io.dbsink.connector.sink.event.SchemaChangeEvent;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.jdbc.StatementBinder;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.relation.TableDefinition;
import io.dbsink.connector.sink.relation.TableDefinitions;
import io.dbsink.connector.sink.relation.TableId;
import io.dbsink.connector.sink.sql.SQLState;
import io.dbsink.connector.sink.trigger.DelayTrigger;
import io.dbsink.connector.sink.trigger.Trigger;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.dbsink.connector.sink.sql.SQLState.ERR_RELATION_EXISTS_ERROR;
import static io.dbsink.connector.sink.sql.SQLState.ERR_RELATION_NOT_EXISTS_ERROR;

/**
 * Jdbc applier, execute dml(insert,update,delete,upsert) or ddl
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
@ThreadSafe
public class JdbcApplier implements Applier<Collection<ChangeEvent>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcApplier.class);

    private final DatabaseDialect databaseDialect;

    private final JdbcConnection jdbcConnection;

    private final TableDefinitions tableDefinitions;

    private final List<DataChangeEvent> events;

    private final Trigger delayTrigger;

    private final ConnectorConfig config;

    private final ConnectorContext context;

    private Map<TopicPartition, OffsetAndMetadata> offsets;

    private StatementBinder statementBinder = null;

    private PreparedStatement preparedStatement = null;

    private Operation operation = null;

    private TableId tableId = null;

    private boolean enableUpsertMode = false;

    public JdbcApplier(ConnectorContext context, ConnectorConfig config) {
        this.context = context;
        this.config = config;
        this.databaseDialect = DatabaseDialects.create(config);
        this.events = new ArrayList<>();
        this.delayTrigger = new DelayTrigger(Duration.ofSeconds(120), new SQLExceptionTrigger());
        this.tableDefinitions = new TableDefinitions(databaseDialect);
        this.jdbcConnection = new JdbcConnection(databaseDialect, config);
    }

    @Override
    public void prepare(TaskContext taskContext) {
        offsets = taskContext.getOffsets();
    }

    private void handleDataChangeEvent(Connection connection, DataChangeEvent event) throws SQLException {
        final TableDefinition tableDefinition = tableDefinitions.get(connection, event.getTableId());
        if (tableDefinition == null) {
            throw new ConnectException("table \"" + event.getTableId() + "\" doesn't exist!");
        }
        Operation operation;
        if (enableUpsertMode && (event.getOperation() == Operation.CREATE || event.getOperation() == Operation.READ)) {
            operation = Operation.UPSERT;
        } else {
            operation = event.getOperation();
        }
        if (!operation.equals(this.operation) || !event.getTableId().equals(tableId)) {
            flush(true);
            this.operation = operation;
            tableId = event.getTableId();
        }
        if (preparedStatement == null) {
            FieldsMetaData fieldsMetaData = event.getFieldsMetaData();
            String statement = buildStatement(fieldsMetaData, tableDefinition);
            preparedStatement = databaseDialect.createPreparedStatement(statement, connection);
            statementBinder = databaseDialect.createStatementBinder(fieldsMetaData, tableDefinition);
        }
        this.events.add(event);
    }

    private void handleSchemaChangeEvent(Connection connection, SchemaChangeEvent event) throws SQLException {
        flush(true);
        databaseDialect.executeDDL(event.getTableId(), event.getDDL(), connection);
    }

    /**
     * apply the change events {@link ChangeEvent} including schema change events {@link SchemaChangeEvent} and
     * data change events {@link DataChangeEvent}
     *
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    @Override
    public synchronized void apply(Collection<ChangeEvent> events) throws ApplierException {
        for (int retry = 0; retry < config.getJdbcRetriesMax(); retry++) {
            try {
                doApply(events);
                return;
            } catch (SQLException e) {
                if (retry < config.getJdbcRetriesMax() - 1) {
                    LOGGER.warn("failed to apply change events, retry to apply", e);
                    try {
                        Thread.sleep(config.getJdbcBackoff());
                    } catch (InterruptedException ex) {
                        if (!context.isRunning()) {
                            throw new ApplierException("connector is closed!");
                        }
                    }
                    continue;
                }
                throw new ApplierException("failed to apply change events", e);
            }
        }
    }

    private void doApply(Collection<ChangeEvent> events) throws SQLException {
        if (events.isEmpty()) {
            return;
        }
        Connection connection = jdbcConnection.connection();
        try {
            connection.setAutoCommit(false);
            for (ChangeEvent event : events) {
                if (event instanceof DataChangeEvent) {
                    handleDataChangeEvent(connection, (DataChangeEvent) event);
                } else if (event instanceof SchemaChangeEvent) {
                    handleSchemaChangeEvent(connection, (SchemaChangeEvent) event);
                } else {
                    LOGGER.debug("ignore the event");
                }
            }
            flush(true);
            connection.commit();
            delayTrigger.reset();
        } catch (SQLException e) {
            rollback(connection);
            if (!ignorable(e)) {
                throw e;
            }
        } finally {
            flush(false);
        }
    }

    private void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException sqlException) {
            LOGGER.debug("failed to rollback, ignore it", sqlException);
        }
    }

    private boolean ignorable(SQLException e) {
        SQLState sqlState = databaseDialect.resolveSQLState(e.getSQLState());
        if (sqlState == SQLState.ERR_DUP_KEY) {
            // when duplicate key sql exception happens, set the trigger
            delayTrigger.set();
            return false;
        }
        if (sqlState == ERR_RELATION_EXISTS_ERROR || sqlState == ERR_RELATION_NOT_EXISTS_ERROR) {
            LOGGER.warn("relation already exists or not exists, ignore it");
            return true;
        }
        return false;
    }

    @Override
    public void release() {
        jdbcConnection.close();
    }

    private void flush(boolean needExecute) throws SQLException {
        if (needExecute && !events.isEmpty()) {
            for (DataChangeEvent event : events) {
                switch (operation) {
                    case DELETE:
                        statementBinder.bindDeleteStatement(preparedStatement, event);
                        break;
                    case UPDATE:
                        statementBinder.bindUpdateStatement(preparedStatement, event);
                        break;
                    case CREATE:
                    case READ:
                        statementBinder.bindInsertStatement(preparedStatement, event);
                        break;
                    case UPSERT:
                        statementBinder.bindUpsertStatement(preparedStatement, event);
                        break;
                    default:
                        throw new IllegalArgumentException("not implement!");
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        preparedStatement = null;
        events.clear();
    }


    private String buildStatement(FieldsMetaData fieldsMetaData, TableDefinition tableDefinition) {
        switch (operation) {
            case UPDATE:
                return databaseDialect.buildUpdateStatement(fieldsMetaData, tableDefinition);
            case UPSERT:
                return databaseDialect.buildUpsertStatement(fieldsMetaData, tableDefinition);
            case CREATE:
            case READ:
                return databaseDialect.buildInsertStatement(fieldsMetaData, tableDefinition);
            case DELETE:
                return databaseDialect.buildDeleteStatement(fieldsMetaData, tableDefinition);
            default:
                throw new IllegalArgumentException("not implement!");
        }
    }

    /**
     * trigger used to handle sql exception {@link SQLException}
     * switch to upsert mode from insert mode when duplicate key
     * sql exception happened
     *
     * @author: Wang Wei
     * @time: 2023-06-25
     */
    private class SQLExceptionTrigger implements Trigger {
        /**
         * Enable upsert mode when duplicate key sql exception {@link SQLException}
         *
         * @author: Wang Wei
         * @time: 2023-06-25
         */
        @Override
        public void set() {
            enableUpsertMode = true;
        }

        /**
         * Disable upsert mode when no more sql exception happens,
         *
         * @author: Wang Wei
         * @time: 2023-06-25
         */
        @Override
        public void reset() {
            enableUpsertMode = false;
        }
    }
}
