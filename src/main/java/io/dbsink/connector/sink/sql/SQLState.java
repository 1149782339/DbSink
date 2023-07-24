/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.sql;

/**
 * SQLState
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
public enum SQLState {
    /**
     * Syntax key error
     */
    ERR_SYNTAX_ERR,
    /**
     * Duplicate key error
     */
    ERR_DUP_KEY,
    /**
     * Schema exists error
     */
    ERR_SCHEMA_EXISTS_ERROR,
    /**
     * Schema not exists error
     */
    ERR_SCHEMA_NOT_EXISTS_ERROR,
    /**
     * Relation exists error
     */
    ERR_RELATION_EXISTS_ERROR,
    /**
     * Relation not exists error
     */
    ERR_RELATION_NOT_EXISTS_ERROR,
    /**
     * Column of relation not exists error
     */
    ERR_COLUMN_OF_RELATION_NOT_EXISTS_ERROR,
    /**
     * Column of relation exists error
     */
    ERR_COLUMN_OF_RELATION_EXISTS_ERROR,
    /**
     * Other unknown sql error
     */
    ERR_UNKNOWN
}
