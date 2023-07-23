/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.util;

import org.apache.kafka.connect.data.Schema;
import org.bson.types.Binary;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * String util
 *
 * @author Wang Wei
 * @time: 2023-06-16
 */
public class StringUtil {
    /**
     * Quote sql identifier with the specified quote
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
     * Quote sql identifier with the specified quote
     *
     * @param identified sql identifier
     * @param quote      quote
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public static String quote(String identified, char quote) {
        if (identified.charAt(0) == quote && identified.charAt(identified.length() - 1) == quote) {
            return identified;
        }
        if (isQuoted(identified)) {
            identified = unquote(identified);
        }
        return quote + identified + quote;
    }

    /**
     * Get the quote character of the SQL identifier if possible
     *
     * @param identifier SQL identifier
     * @return the quote character if possible
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public static Optional<Character> getQuote(String identifier) {
        if (isQuoted(identifier)) {
            return Optional.of(identifier.charAt(0));
        }
        return Optional.empty();
    }

    /**
     * Remove the quote from a SQL identifier with the specified quote
     *
     * @param identifier SQL identifier
     * @param quote      quote
     * @return SQL identifier without the quote
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public static String unquote(String identifier, char quote) {
        if (identifier.charAt(0) == quote && identifier.charAt(identifier.length() - 1) == quote) {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    /**
     * Remove the quote from a SQL identifier
     *
     * @param identifier SQL identifier
     * @return SQL identifier without the quote
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public static String unquote(String identifier) {
        Optional<Character> optional = getQuote(identifier);
        if (!optional.isPresent()) {
            return identifier;
        }
        char quota = optional.get();
        identifier.replace(quota + "" + quota, quota + "");
        return identifier.substring(1, identifier.length() - 1);
    }

    /**
     * judge if a SQL identifier is quoted
     *
     * @param identifier
     * @return if a SQL identifier is quoted
     * @author Wang Wei
     * @time: 2023-07-22
     */
    public static boolean isQuoted(String identifier) {
        if (identifier.length() <= 2) {
            return false;
        }
        if (identifier.charAt(0) == '"' && identifier.charAt(identifier.length() - 1) == '"') {
            return true;
        }
        if (identifier.charAt(0) == '`' && identifier.charAt(identifier.length() - 1) == '`') {
            return true;
        }
        if (identifier.charAt(0) == '\'' && identifier.charAt(identifier.length() - 1) == '\'') {
            return true;
        }
        return false;
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
