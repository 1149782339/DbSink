/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.event.*;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.relation.FieldsMetaData;
import io.dbsink.connector.sink.transaction.Transaction;
import io.dbsink.connector.sink.util.ErrorHandler;
import io.dbsink.connector.sink.util.WriteSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Parallel transaction applier, packets change events as transactions,
 * and parallel applies these transactions by using write set algorithm
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
public class ParallelTxApplier implements Applier<Collection<ChangeEvent>> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ParallelTxApplier.class);

    private final BlockingQueue<ApplierEvent> queue;

    private final ParallelDispatcher dispatcher;

    private final ConnectorContext connectorContext;
    private final ErrorHandler errorHandler;

    private final Map<String, Transaction> transactionMap = new LinkedHashMap<>();

    private long lastCommitted;

    private long sequenceNumber;

    private String transactionId = null;

    private WriteSet writeSet = new WriteSet();

    private ConnectorConfig config;

    public ParallelTxApplier(
        ConnectorContext connectorContext,
        ErrorHandler errorHandler,
        ConnectorConfig config
    ) {
        this.connectorContext = connectorContext;
        this.errorHandler = errorHandler;
        this.lastCommitted = 0L;
        this.sequenceNumber = 1L;
        this.config = config;
        this.queue = new ArrayBlockingQueue<>(config.getTransactionBufferSize());
        this.dispatcher = new ParallelDispatcher(queue, connectorContext, errorHandler, config);
    }

    @Override
    public void prepare(TaskContext taskContext) {
        dispatcher.prepare(taskContext);
        dispatcher.start();
    }

    @Override
    public void apply(Collection<ChangeEvent> events) throws ApplierException {
        try {
            for (ChangeEvent event : events) {
                String transactionId = event.getTransactionId();
                if (transactionId == null) {
                    dispatchApplierEvent(Transaction.of(event));
                    this.transactionId = null;
                    continue;
                }
                if (event instanceof TransactionEvent) {
                    handleTransactionEvent((TransactionEvent) event);
                    continue;
                }
                if (this.transactionId == null) {
                    this.transactionId = transactionId;
                }
                Transaction transaction = transactionMap.get(transactionId);
                if (transaction == null) {
                    transaction = new Transaction(transactionId);
                    transactionMap.put(transactionId, transaction);
                }
                transaction.addEvent(event);
                if (!transactionId.equals(this.transactionId)) {
                    transactionMap.remove(transactionId);
                    dispatchApplierEvent(transaction);
                }
                this.transactionId = transactionId;
            }
        } catch (InterruptedException e) {
            LOGGER.warn("InterruptedException");
            throw new ApplierException("failed to apply", e);
        }
    }

    private void handleTransactionEvent(TransactionEvent event) throws InterruptedException {
        TransactionEvent.Status status = event.getStatus();
        String transactionId = event.getTransactionId();
        Transaction transaction;
        if (status == TransactionEvent.Status.BEGIN) {
            transaction = new Transaction(transactionId);
            transaction.addEvent(event);
            transactionMap.put(transactionId, transaction);
            this.transactionId = transactionId;
            return;
        }
        transaction = transactionMap.remove(transactionId);
        if (transaction == null) {
            return;
        }
        transaction.addEvent(event);
        dispatchApplierEvent(transaction);
    }

    private void dispatchApplierEvent(Transaction transaction) throws InterruptedException {
        boolean canUseWriteSet = true;
        // if there is a dll event  or a dml event involving any tables
        // without primary keys in the transaction, in this case, write set
        // can't be used
        for (ChangeEvent event : transaction.getEvents()) {
            if (event instanceof DataChangeEvent) {
                FieldsMetaData fieldsMetaData = ((DataChangeEvent) event).getFieldsMetaData();
                if (fieldsMetaData.getPrimaryKeyFieldNames().isEmpty()) {
                    canUseWriteSet = false;
                    break;
                }
            } else if (event instanceof SchemaChangeEvent) {
                canUseWriteSet = false;
                break;
            }
        }
        // distribute lastCommitted and sequenceNumber for a transactional event
        ApplierEvent applierEvent = ApplierEvent
            .builder()
            .lastCommitted(lastCommitted++)
            .sequenceNumber(sequenceNumber++)
            .records(transaction.getEvents())
            .transactionId(transaction.getId())
            .canUseWriteSet(canUseWriteSet)
            .hasDataChangeEvent(transaction.hasDataChangeEvent())
            .build();
        if (applierEvent.hasDataChangeEvent()) {
            writeSet.add(applierEvent);
        }
        queue.put(applierEvent);
    }


    @Override
    public void release() {
        dispatcher.stop();
        dispatcher.release();
    }
}
