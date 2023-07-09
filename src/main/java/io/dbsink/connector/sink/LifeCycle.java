/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink;

import io.dbsink.connector.sink.context.TaskContext;

/**
 * Common interface for component life cycle methods.
 * Each component must implement this interface
 *
 * @author: Wang Wei
 * @time: 2023-06-09
 */
public interface LifeCycle {

    /**
     * Prepare resources
     * this method must be called before calling apply method
     *
     * @param taskContext task context  {@link TaskContext}
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void prepare(TaskContext taskContext);

    /**
     * Release the resources
     * this method must be called when the object is no more used
     *
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void release();
}
