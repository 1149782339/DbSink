/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.event;

import java.util.Objects;

/**
 * Operation equivalent to #io.debezium.data.Envelope.Operation
 *
 * @author: Wang Wei
 * @time: 2023-06-10
 */
public enum Operation {
    // The Upsert up is dummy, because debezium only has four operations
    // in DataChangeEvent,which are READ,CREATE,UPDATE,DELETE,
    // it is only used when duplicate key errors need to be handled.
    UPSERT("dummy"),
    READ("r"),
    CREATE("c"),
    UPDATE("u"),
    DELETE("d");
    private final String op;

    Operation(String op) {
        this.op = op;
    }

    /**
     * Get operation from string
     *
     * @param op Operation
     * @return operation
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public static Operation fromString(String op) {
        for (Operation operation : Operation.values()) {
            if (Objects.equals(op, operation.op)) {
                return operation;
            }
        }
        throw new IllegalArgumentException("\"" + op + "\" is not a valid operation");
    }
}
