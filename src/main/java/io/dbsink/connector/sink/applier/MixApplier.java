/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.Operation;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.util.ErrorHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Mix applier can do transaction based parallel
 * applying and table based parallel applying.
 *
 *
 * @author: Wang Wei
 * @time: 2023-07-07
 */
public class MixApplier implements Applier<Collection<ChangeEvent>> {

    private final Applier parallelTableApplier;

    private final Applier parallelTxApplier;

    private final boolean transactionEnabled;

    public MixApplier(ConnectorContext context, ErrorHandler errorHandler, ConnectorConfig config) {
        this.parallelTableApplier = new ParallelTableApplier(context, config);
        this.parallelTxApplier = new ParallelTxApplier(context, errorHandler, config);
        this.transactionEnabled = config.getTransactionEnabled();
    }

    @Override
    public void prepare(TaskContext taskContext) {
        parallelTableApplier.prepare(taskContext);
        parallelTxApplier.prepare(taskContext);
    }

    @Override
    public void release() {
        parallelTxApplier.release();
        parallelTableApplier.release();
    }

    @Override
    public synchronized void apply(Collection<ChangeEvent> changeEvents) throws ApplierException {
        if (!transactionEnabled) {
            parallelTableApplier.apply(changeEvents);
            return;
        }
        // Snapshot events(read events) are not transactional and can't be applied by TxApplier.
        // So These events need to be divided into snapshot events and non snapshot events,
        // which can applied by different appliers.
        // By the way, debezium now supports table parallel snapshot(see #snapshot.max.threads),
        // parallelTableApplier will do better performance
        List<ChangeEvent> snapshotEvents = new ArrayList<>(changeEvents.size());
        List<ChangeEvent> nonSnapshotEvents = new ArrayList<>(changeEvents.size());
        for (ChangeEvent changeEvent : changeEvents) {
            if (changeEvent instanceof DataChangeEvent) {
                DataChangeEvent dataChangeEvent = (DataChangeEvent) changeEvent;
                if (dataChangeEvent.getOperation() == Operation.READ) {
                    snapshotEvents.add(dataChangeEvent);
                } else {
                    nonSnapshotEvents.add(changeEvent);
                }
            } else {
                nonSnapshotEvents.add(changeEvent);
            }
        }
        parallelTableApplier.apply(snapshotEvents);
        parallelTxApplier.apply(nonSnapshotEvents);
    }
}
