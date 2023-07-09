/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.context;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Task Context
 *
 * @author: Wang Wei
 * @time: 2023-06-17
 */
public class TaskContext {

    /* used to flush kafka offsets */
    private Map<TopicPartition, OffsetAndMetadata> offsets;

    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return offsets;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<TopicPartition, OffsetAndMetadata> offsets;

        public Builder offsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
            return this;
        }

        public TaskContext build() {
            TaskContext taskContext = new TaskContext();
            taskContext.offsets = offsets;
            return taskContext;
        }

    }
}
