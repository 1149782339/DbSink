/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.ConnectorConfig;
import io.dbsink.connector.sink.annotation.ThreadSafe;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.event.ChangeEvent;
import io.dbsink.connector.sink.event.DataChangeEvent;
import io.dbsink.connector.sink.event.TransactionEvent;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.relation.TableId;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Parallel table applier
 *
 * @author: Wang Wei
 * @time: 2023-07-06
 */
@ThreadSafe
public class ParallelTableApplier implements Applier<Collection<ChangeEvent>> {
    private final static Logger LOGGER = LoggerFactory.getLogger(ParallelTableApplier.class);

    private final static long MAX_AWAIT_TIME = 60;

    private final CompletionService completionService;

    private final ExecutorService executorService;

    private final List<Applier> appliers;

    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public ParallelTableApplier(ConnectorContext context, ConnectorConfig config) {
        this.executorService = Executors.newFixedThreadPool(config.getApplierParallelMax());
        this.completionService = new ExecutorCompletionService<>(executorService);
        this.appliers = new ArrayList<>(config.getApplierParallelMax());
        for (int i = 0; i < config.getApplierParallelMax(); i++) {
            this.appliers.add(ApplierFactory.createTableApplier(context, config));
        }
    }


    @Override
    public void prepare(TaskContext taskContext) {
        offsets = taskContext.getOffsets();
        for (Applier applier : appliers) {
            applier.prepare(taskContext);
        }
    }

    @Override
    public synchronized void release() {
        for (Applier applier : appliers) {
            applier.release();
        }
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(TimeUnit.SECONDS.toSeconds(MAX_AWAIT_TIME), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.debug("interrupted when stop,ignore it", e);
        }
    }

    @Override
    public synchronized void apply(Collection<ChangeEvent> changeEvents) throws ApplierException {
        Map<TableId, List<ChangeEvent>> tableEvents = new HashMap<>();
        Map<String, ChangeEvent> topics = new HashMap<>();
        for (ChangeEvent changeEvent : changeEvents) {
            topics.put(changeEvent.getTopic(), changeEvent);
            if (changeEvent instanceof TransactionEvent) {
                continue;
            }
            if (changeEvent instanceof DataChangeEvent) {
                DataChangeEvent dataChangeEvent = (DataChangeEvent) changeEvent;
                TableId tableId = dataChangeEvent.getTableId();
                List<ChangeEvent> events = tableEvents.get(tableId);
                if (events == null) {
                    events = new LinkedList<>();
                    tableEvents.put(tableId, events);
                }
                events.add(dataChangeEvent);
            }
        }
        for (Map.Entry<TableId, List<ChangeEvent>> entry : tableEvents.entrySet()) {
            final TableId tableId = entry.getKey();
            final List<ChangeEvent> events = entry.getValue();
            final Applier applier = appliers.get(Math.abs(tableId.hashCode()) % appliers.size());
            completionService.submit(() -> {
                applier.apply(events);
                return null;
            });
        }
        try {
            for (List<ChangeEvent> ignored : tableEvents.values()) {
                completionService.take().get();
            }
            refreshProcessedRecordOffset(topics);
        } catch (InterruptedException | ExecutionException e) {
            throw new ApplierException(e);
        }
    }

    private void refreshProcessedRecordOffset(Map<String, ChangeEvent> topics) {
        for (ChangeEvent event : topics.values()) {
            String topic = event.getTopic();
            Integer partition = event.getPartition();
            long offset = event.getOffset();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
            LOGGER.debug("refresh processed record, topic:{}, partition:{}, offset:{}", topic, partition, offset);
            offsets.put(topicPartition, offsetAndMetadata);
        }
    }
}
