/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.relation;

import io.dbsink.connector.sink.sql.ExpressibleObject;
import io.dbsink.connector.sink.util.StringUtil;
import io.dbsink.connector.sink.sql.ExpressionBuilder;
import io.dbsink.connector.sink.sql.IdentifierRule;

import java.util.Optional;

/**
 * Column definition
 *
 * @author Wang Wei
 * @time: 2023-06-06
 */
public class ColumnDefinition implements ExpressibleObject {
    /**
     * Column name
     */
    private String name;
    /**
     * Type name
     */
    private String typeName;
    /**
     * Jdbc type
     */
    private int jdbcType;
    /**
     * Column length
     */
    private int length;
    /**
     * Scale
     */
    private Optional<Integer> scale;
    /**
     * Is generated column
     */
    private boolean isGenerated;
    /**
     * Column position
     */
    private int position;

    /**
     * Get column name
     *
     * @return column name
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public String getName() {
        return name;
    }

    /**
     * Get type name
     *
     * @return type name
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Get jdbc type
     *
     * @return jdbc type
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public int getJdbcType() {
        return jdbcType;
    }

    /**
     * Get column length
     *
     * @return column length
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public int getLength() {
        return length;
    }

    /**
     * Get column scale
     *
     * @return column scale
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public Optional<Integer> getScale() {
        return scale;
    }

    /**
     * Get if column is generated
     *
     * @return column scale
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public boolean isGenerated() {
        return isGenerated;
    }

    /**
     * Get column position index
     *
     * @return column position index
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public int getPosition() {
        return position;
    }

    /**
     * Append to expression builder, column should be quoted with the quoteString
     *
     * @param builder expression builder {@link ExpressionBuilder}
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    @Override
    public void append(ExpressionBuilder builder) {
        final IdentifierRule rule = builder.getIdentifierRule();
        final String quoteString = rule.getQuoteString();
        builder.append(StringUtil.quote(name, quoteString));
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Get Column Definition builder {@link Builder} to build Column Definition {@link ColumnDefinition}
     *
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Column Definition builder to build Column Definition {@link ColumnDefinition}
     *
     * @author: Wang Wei
     * @time: 2023-06-06
     */
    public static class Builder {
        /**
         * Column name
         */
        private String name;
        /**
         * Type name
         */
        private String typeName;
        /**
         * Jdbc type
         */
        private int jdbcType;
        /**
         * Column length
         */
        private int length;
        /**
         * Scale
         */
        private Optional<Integer> scale;
        /**
         * Is generated column
         */
        private boolean isGenerated;
        /**
         * Column position
         */
        private int position;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder jdbcType(int jdbcType) {
            this.jdbcType = jdbcType;
            return this;
        }

        public Builder length(int length) {
            this.length = length;
            return this;
        }

        public Builder scale(Optional<Integer> scale) {
            this.scale = scale;
            return this;
        }


        public Builder isGenerated(boolean isGenerated) {
            this.isGenerated = isGenerated;
            return this;
        }

        public Builder position(int position) {
            this.position = position;
            return this;
        }

        public ColumnDefinition build() {
            ColumnDefinition columnDefinition = new ColumnDefinition();
            columnDefinition.name = name;
            columnDefinition.typeName = typeName;
            columnDefinition.jdbcType = jdbcType;
            columnDefinition.length = length;
            columnDefinition.scale = scale;
            columnDefinition.isGenerated = isGenerated;
            columnDefinition.position = position;
            return columnDefinition;
        }
    }

}
