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
import io.dbsink.connector.sink.util.TimeTracker;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

    private static final int MAX_UPSERT_ELAPSED_TIME = 120000;

    private final DatabaseDialect databaseDialect;

    private final JdbcConnection jdbcConnection;

    private final TableDefinitions tableDefinitions;

    private final List<DataChangeEvent> events;

    private final ConnectorConfig config;

    private final ConnectorContext context;

    private Map<TopicPartition, OffsetAndMetadata> offsets;

    private StatementBinder statementBinder = null;

    private PreparedStatement preparedStatement = null;

    private Operation operation = null;

    private TableId tableId = null;

    private boolean enableUpsertMode = false;

    private final TimeTracker timeTracker;

    public JdbcApplier(ConnectorContext context, ConnectorConfig config) {
        this.context = context;
        this.config = config;
        this.databaseDialect = DatabaseDialects.create(config);
        this.events = new ArrayList<>();
        this.timeTracker = new TimeTracker();
        this.tableDefinitions = new TableDefinitions(databaseDialect);
        this.jdbcConnection = new JdbcConnection(databaseDialect, config);
    }

    @Override
    public void prepare(TaskContext taskContext) {
        offsets = taskContext.getOffsets();
    }

    private void handleDataChangeEvent(Connection connection, DataChangeEvent event) throws SQLException {
        final TableId tableId = databaseDialect.resolveTableId(event.getTableId());
        final TableDefinition tableDefinition = tableDefinitions.get(connection, tableId);
        if (tableDefinition == null) {
            throw new ConnectException("table \"" + tableId + "\" doesn't exist!");
        }
        Operation operation;
        if (enableUpsertMode && (event.getOperation() == Operation.CREATE || event.getOperation() == Operation.READ)) {
            operation = Operation.UPSERT;
        } else {
            operation = event.getOperation();
        }
        if (this.operation == null) {
            this.operation = event.getOperation();
        }
        if (this.tableId == null) {
            this.tableId = tableId;
        }
        if (!operation.equals(this.operation) || !tableId.equals(this.tableId)) {
            flush(true);
            this.operation = operation;
            this.tableId = tableId;
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
        for (int retry = 0; retry < config.getConnectionRetriesMax(); retry++) {
            try {
                doApply(events);
                return;
            } catch (SQLException e) {
                if (retry < config.getConnectionRetriesMax() - 1) {
                    LOGGER.warn("failed to apply change events, retry to apply", e);
                    try {
                        Thread.sleep(config.getConnectionBackoff());
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
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
            if (enableUpsertMode && timeTracker.isTimeElapsed(MAX_UPSERT_ELAPSED_TIME)) {
                enableUpsertMode = false;
            }
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
            // when duplicate key sql exception happens, switch to upsert mode
            enableUpsertMode = true;
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
}
