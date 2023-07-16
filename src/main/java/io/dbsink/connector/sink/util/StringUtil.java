/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import org.apache.kafka.connect.data.Schema;
import org.bson.types.Binary;

import java.nio.ByteBuffer;

/**
 * String util
 *
 * @author Wang Wei
 * @time: 2023-06-16
 */
public class StringUtil {
    /**
     * Quote sql identifier
     *
     * @param identifier sql identifier
     * @param quote      quote
     * @author Wang Wei
     * @time: 2023-06-18
     */
    public static String quote(String identifier, String quote) {
        return quote + identifier.replace(quote, quote + quote) + quote;
    }

    /**
     * judge if string object is empty
     *
     * @param str string object
     * @return if string object is empty
     * @author Wang Wei
     * @time: 2023-06-18
     */
    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static String toString(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        switch (schema.type()) {
            case STRING:
                return (String) value;
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case INT32:
            case INT16:
            case INT8:
                return value.toString();
        }
        return value.toString();
    }
}
