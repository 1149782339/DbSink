/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.context;


/**
 * Connector Context
 *
 * @author: Wang Wei
 * @time: 2023-06-17
 */
public interface ConnectorContext {
    /**
     * Judge if connector task is running
     *
     * @return if connector task is running
     * @author: Wang Wei
     * @time: 2023-06-17
     */
    boolean isRunning();
}
