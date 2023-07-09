/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

/**
 * Timestamp util
 *
 * @author Wang Wei
 * @time: 2023-06-18
 */
public class TimestampUtil {
    /**
     * Get current timestamp
     * @return current timestamp
     * @author Wang Wei
     * @time: 2023-06-16
     */
    public static long currentTimestamp() {
        return System.currentTimeMillis();
    }
}
