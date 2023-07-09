/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.applier;

import io.dbsink.connector.sink.LifeCycle;
import io.dbsink.connector.sink.exception.ApplierException;

/**
 * Applier interface defines the lifecycle and the function of an applier
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public interface Applier<T> extends LifeCycle {
    /**
     * Apply the event
     * this method must be called after calling prepare method
     *
     * @param applierEvent applier event
     * @throws ApplierException if there is an error applying
     * @author: Wang Wei
     * @time: 2023-06-09
     */
    void apply(T applierEvent) throws ApplierException;
}
