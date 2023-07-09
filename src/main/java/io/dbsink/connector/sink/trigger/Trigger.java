/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.trigger;


/**
 * Trigger
 *
 * @author: Wang Wei
 * @time: 2023-06-15
 */
public interface Trigger {
    /**
     * Set to active state
     *
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    void set();
    /**
     * Set to inactive state
     *
     * @author: Wang Wei
     * @time: 2023-06-15
     */
    void reset();
}
