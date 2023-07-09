/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.exception;

/**
 * Applier Exception
 *
 * @author Wang Wei
 * @time: 2023-06-09
 */
public class ApplierException extends Exception {
    public ApplierException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApplierException(String message) {
        super(message);
    }

    public ApplierException(Throwable cause) {
        super(cause);
    }
}
