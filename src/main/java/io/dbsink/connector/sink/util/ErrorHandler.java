/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Error handler
 *
 * @author Wang Wei
 * @time: 2023-06-18
 */
public class ErrorHandler {

    private volatile Throwable throwable;

    /**
     * Set exception
     *
     * @param throwable exception
     * @author Wang Wei
     * @time: 2023-06-18
     */
    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    /**
     * Throw exception
     *
     * @author Wang Wei
     * @time: 2023-06-18
     */
    public void throwThrowable() {
        if (throwable != null) {
            throw new ConnectException(throwable);
        }
    }
}
