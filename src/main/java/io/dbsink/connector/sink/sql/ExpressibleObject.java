/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.sql;


/**
 * Expressible Object
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public interface ExpressibleObject {

    /**
     * Append text to the builder {@link ExpressionBuilder}
     *
     * @param builder {@link ExpressionBuilder}
     * @author Wang Wei
     * @time: 2023-06-10
     */
    void append(ExpressionBuilder builder);
}
