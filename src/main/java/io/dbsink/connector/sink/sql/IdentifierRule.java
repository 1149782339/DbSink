/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.sql;

/**
 * Identifier Rule
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public class IdentifierRule {
    /**
     * delimiter
     *
     */
    private final String delimiter;
    /**
     * quote string
     *
     */
    private final String quoteString;

    /**
     * IdentifierRule
     *
     * @param delimiter   delimiter
     * @param quoteString quote string
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public IdentifierRule(String delimiter, String quoteString) {
        this.delimiter = delimiter;
        this.quoteString = quoteString;
    }

    /**
     * Get delimiter string
     *
     * @return delimiter
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * Get quote string
     *
     * @return quote string
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public String getQuoteString() {
        return quoteString;
    }
}
