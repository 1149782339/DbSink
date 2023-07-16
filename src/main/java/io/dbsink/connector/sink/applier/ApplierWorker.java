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
import io.dbsink.connector.sink.util.ErrorHandler;
import io.dbsink.connector.sink.util.WorkerThread;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Runtime Applier worker using jdbc applier {@link JdbcApplier} to apply events {@link ApplierEvent}
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public class ApplierWorker extends WorkerThread implements LifeCycle {

    private final static Logger LOGGER = LoggerFactory.getLogger(ApplierWorker.class);
    private final BlockingQueue<ApplierEvent> applierEvents;

    private final ParallelDispatcher dispatcher;

    private final Applier tableApplier;

    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public ApplierWorker(
        ParallelDispatcher dispatcher,
        ConnectorContext context,
        ErrorHandler errorHandler,
        ConnectorConfig config
    ) {
        super(context, errorHandler);
        this.applierEvents = new ArrayBlockingQueue<>(config.getApplierWorkerBufferSize());
        this.dispatcher = dispatcher;
        this.tableApplier = ApplierFactory.createTableApplier(context, config);
    }

    /**
     * Runtime Applier worker using jdbc applier,
     *
     * @param applierEvent applier event  {@link ApplierEvent}
     * @throws InterruptedException if there is interrupted while put applier event {@link ApplierEvent} into the queue
     * @author Wang Wei
     * @time: 2023-06-09
     */
    public void feed(ApplierEvent applierEvent) throws InterruptedException {
        applierEvents.put(applierEvent);
    }

    /**
     * Applier worker main function
     * take events {@link ApplierEvent} from queue, and feed it to jdbc applier {@link JdbcApplier}
     *
     * @throws Exception if there is any exception while applying events
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    @Override
    protected void execute() throws Exception {
        while (context.isRunning()) {
            ApplierEvent applierEvent = applierEvents.take();
            Long sequenceNumber = applierEvent.getSequenceNumber();
            if (applierEvent.hasDataChangeEvent()) {
                tableApplier.apply(applierEvent.getEvents());
            }
            Long minSequenceNumber = dispatcher.updateSequenceNoAndGet(sequenceNumber);
            // Only the applier worker who has applied the transaction with the minimum sequence number
            // can update kafka consumed offset. Otherwise, there may be some transactions
            // with the smaller sequence number that have not been applied completely, in this case,
            // if connector process crashes and restarts, these records of transactions will not be consumed,
            // because a larger offset has been committed.
            if (minSequenceNumber.compareTo(sequenceNumber) >= 0) {
                refreshProcessedRecordOffset(applierEvent);
            }
        }
    }

    private void refreshProcessedRecordOffset(ApplierEvent applierEvent) {
        String topic = applierEvent.getTopic();
        Integer partition = applierEvent.getPartition();
        long offset = applierEvent.getOffset();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
        LOGGER.debug("refresh processed record, topic:{}, partition:{}, offset:{}", topic, partition, offset);
        offsets.put(topicPartition, offsetAndMetadata);
    }

    /**
     * Prepare jdbc applier {@link JdbcApplier} for applying events {@link ApplierEvent}
     * this method must be called before calling start method
     *
     * @param taskContext task context  {@link TaskContext}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    @Override
    public void prepare(TaskContext taskContext) {
        tableApplier.prepare(taskContext);
        offsets = taskContext.getOffsets();
    }

    /**
     * Get Applier event queue size {@link BlockingQueue}
     *
     * @return applier event queue size {@link BlockingQueue}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    public int getApplierEventQueueSize() {
        return applierEvents.size();
    }

    /**
     * Release jdbc applier {@link JdbcApplier}
     * this method must be called after calling stop method
     *
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    @Override
    public void release() {
        tableApplier.release();
    }
}
