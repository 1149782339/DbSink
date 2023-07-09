/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

/**
 * Boolean Util
 *
 * @author Wang Wei
 * @time: 2023-07-08
 */
public class BooleanUtil {
    /**
     * Get boolean from int value
     *
     * @param value value
     * @return boolean value
     * @author Wang Wei
     * @time: 2023-06-16
     */
    public static boolean toBoolean(int value) {
        if (value == 1) {
            return true;
        } else if (value == 0) {
            return false;
        } else {
            throw new IllegalArgumentException("value \"" + value + "\" can't be converted to boolean");
        }
    }
}
