/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.LifeCycle;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.event.ApplierEvent;
import io.dbsink.connector.sink.transaction.SequenceNo;
import io.dbsink.connector.sink.util.ErrorHandler;
import io.dbsink.connector.sink.util.WorkerThread;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * dispatch transaction event to applier worker {@link ApplierWorker}
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
public class ParallelDispatcher extends WorkerThread implements LifeCycle {

    private BlockingQueue<ApplierEvent> queue;

    private final SequenceNo maxCommittedSeqNo;

    private Long maxProcessedSeqNo = 0L;

    private final Queue<Long> processingSeqNoQueue;

    private List<ApplierWorker> applierWorkers;

    private ReentrantLock lock = new ReentrantLock();

    private ConnectorConfig config;


    public ParallelDispatcher(
        BlockingQueue<ApplierEvent> queue,
        ConnectorContext context,
        ErrorHandler errorHandler,
        ConnectorConfig config
    ) {
        super(context, errorHandler);
        this.queue = queue;
        this.maxCommittedSeqNo = new SequenceNo();
        this.processingSeqNoQueue = new LinkedList<>();
        this.config = config;
    }

    @Override
    public void execute() throws Exception {
        while (context.isRunning()) {
            ApplierEvent applierEvent = queue.take();
            Long lastCommitted = applierEvent.getLastCommitted();
            Long sequenceNumber = applierEvent.getSequenceNumber();
            if (maxProcessedSeqNo == 0) {
                maxCommittedSeqNo.setSequenceNumber(lastCommitted);
            }
            maxCommittedSeqNo.waitSeqNoGreatEquals(lastCommitted);
            lock.lock();
            processingSeqNoQueue.add(sequenceNumber);
            maxProcessedSeqNo = sequenceNumber;
            lock.unlock();
            ApplierWorker applierWorker = advise();
            applierWorker.feed(applierEvent);
        }
    }

    /**
     * Update sequence number when applier workers {@link ApplierWorker} have applied a transaction
     * and return the min sequence number of the queue.
     * @param sequenceNo transaction sequence number
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    public Long updateSequenceNoAndGet(Long sequenceNo) {
        try {
            lock.lock();
            Long maxSequenceNoTmp = maxProcessedSeqNo;
            for (Long processingSeqNo : processingSeqNoQueue) {
                if (processingSeqNo.equals(sequenceNo)) {
                    processingSeqNoQueue.remove(sequenceNo);
                    break;
                }
            }
            Long processingSeqNo = processingSeqNoQueue.peek();
            if (processingSeqNo != null) {
                maxCommittedSeqNo.setSequenceNumber(processingSeqNo - 1);
            } else {
                maxCommittedSeqNo.setSequenceNumber(maxSequenceNoTmp);
            }
            return processingSeqNo != null ? processingSeqNo : maxSequenceNoTmp;
        } finally {
            lock.unlock();
        }

    }

    /**
     * Advise an applier worker {@link ApplierWorker} to apply a transaction,
     * the policy is to choose the
     *
     * @return applier worker {@link ApplierWorker}
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    private ApplierWorker advise() {
        ApplierWorker applierWorker = applierWorkers.get(0);
        for (int i = 1; i < applierWorkers.size(); i++) {
            if (applierWorkers.get(i).getApplierEventQueueSize() < applierWorker.getApplierEventQueueSize()) {
                applierWorker = applierWorkers.get(i);
            }
        }
        return applierWorker;
    }

    /**
     * Prepare applier workers {@link ApplierWorker} to apply transactions
     *
     * @param taskContext task context  {@link TaskContext}
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    @Override
    public void prepare(TaskContext taskContext) {
        applierWorkers = new ArrayList<>(config.getApplierParallelMax());
        for (int i = 0; i < config.getApplierParallelMax(); i++) {
            ApplierWorker applierWorker = new ApplierWorker(this, context, errorHandler, config);
            applierWorker.prepare(taskContext);
            applierWorker.start();
            applierWorkers.add(applierWorker);
        }
    }

    /**
     * Release applier workers {@link ApplierWorker}
     *
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    @Override
    public void release() {
        for (ApplierWorker applierWorker : applierWorkers) {
            applierWorker.stop();
        }
        for (ApplierWorker applierWorker : applierWorkers) {
            applierWorker.release();
        }
    }
}
