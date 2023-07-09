/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink;

import io.dbsink.connector.sink.applier.ChangeEventApplier;
import io.dbsink.connector.sink.applier.MixApplier;
import io.dbsink.connector.sink.context.ConnectorContext;
import io.dbsink.connector.sink.context.TaskContext;
import io.dbsink.connector.sink.exception.ApplierException;
import io.dbsink.connector.sink.util.ErrorHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DbSinkTask
 *
 * @author: Wang Wei
 * @time: 2023-06-17
 */
public class DbSinkTask extends SinkTask {

    private final Logger LOGGER = LoggerFactory.getLogger(DbSinkTask.class);

    private ConnectorConfig config;

    private ChangeEventApplier applier;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

    private final ErrorHandler errorHandler = new ErrorHandler();

    private volatile boolean isRunning;

    private ConnectorContext context;

    private TaskContext taskContext;


    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        config = new ConnectorConfig(props);
        isRunning = true;
        context = () -> isRunning;
        taskContext = TaskContext.builder()
            .offsets(offsets)
            .build();
        applier = new ChangeEventApplier(new MixApplier(context, errorHandler, config), config);
        applier.prepare(taskContext);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        errorHandler.throwThrowable();
        try {
            applier.apply(records);
        } catch (ApplierException e) {
            LOGGER.error("failed to apply", e);
            throw new ConnectException(e);
        }

    }

    @Override
    public void stop() {
        isRunning = false;
        if (applier != null) {
            applier.release();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        flush(offsets);
        return offsets;
    }

}
