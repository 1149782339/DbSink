/*
 *  Copyright DbSink Authors.
 *  This source code is licensed under the Apache License Version 2.0, available
 *  at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.dbsink.connector.sink.sql;

import io.dbsink.connector.sink.annotation.NotThreadSafe;

import java.util.Iterator;
import java.util.List;

/**
 * Expressible Builder
 *
 * @author Wang Wei
 * @time: 2023-06-10
 */
@NotThreadSafe
public class ExpressionBuilder {
    private final StringBuilder sb;
    private final IdentifierRule identifierRule;


    /**
     * Expression Builder
     *
     * @param identifierRule {@link IdentifierRule}
     * @author: Wang Wei
     * @time: 2023-06-10
     */
    public ExpressionBuilder(IdentifierRule identifierRule) {
        this.sb = new StringBuilder();
        this.identifierRule = identifierRule;
    }

    /**
     * Append object to the builder
     *
     * @param object
     * @return expression builder {@link ExpressionBuilder}
     * @author Wang Wei
     * @time: 2023-06-10
     */
    public ExpressionBuilder append(Object object) {
        if (object instanceof ExpressibleObject) {
            ((ExpressibleObject) object).append(this);
            return this;
        }
        sb.append(object);
        return this;
    }

    /**
     * Add the same object multiple times and separate them with a separator
     *
     * @param delimiter delimiter
     * @param value     value
     * @param count     add count
     * @return expression builder {@link ExpressionBuilder}
     * @author Wang Wei
     * @time: 2023-06-10
     */
    public <T> ExpressionBuilder appendMulti(String delimiter, T value, int count) {
        for (int i = 0; i < count; i++) {
            append(value);
            if (i < count - 1) {
                append(delimiter);
            }
        }
        return this;
    }

    /**
     * Add multiple objects and separate them with a separator
     *
     * @param delimiter delimiter
     * @param objects   objects
     * @return expression builder {@link ExpressionBuilder}
     * @author Wang Wei
     * @time: 2023-06-10
     */
    public <T> ExpressionBuilder appendMulti(String delimiter, List<T> objects) {
        for (int i = 0; i < objects.size(); i++) {
            append(objects.get(i));
            if (i < objects.size() - 1) {
                append(delimiter);
            }
        }
        return this;
    }

    /**
     * Get identifier rule
     *
     * @return identifier rule {@link IdentifierRule}
     * @author Wang Wei
     * @time: 2023-06-10
     */
    public IdentifierRule getIdentifierRule() {
        return identifierRule;
    }

    /**
     * Return list expression builder
     *
     * @return list expression builder {@link ListBuilder}
     * @author Wang Wei
     * @time: 2023-06-10
     */
    public ListBuilder listBuilder() {
        return new ListBuilder(this, null, identifierRule.getDelimiter());
    }

    @Override
    public String toString() {
        return sb.toString();
    }

    /**
     * Expression object transformer interface, used to rewrite append method of an expressible object
     *
     * @author Wang Wei
     * @time: 2023-06-10
     */
    @FunctionalInterface
    public interface Transformer<T> {

        /**
         * Apply object to the builder {@link ExpressionBuilder}
         *
         * @param builder expression builder {@link ExpressionBuilder}
         * @param object  object
         * @author Wang Wei
         * @time: 2023-06-10
         */
        void apply(ExpressionBuilder builder, T object);
    }

    /**
     * List Expressible Object Builder
     *
     * @author Wang Wei
     * @time: 2023-06-10
     */
    public static class ListBuilder {
        private final String delimiter;

        private final Transformer transformer;

        private final ExpressionBuilder builder;

        private boolean first = true;

        /**
         * List builder
         *
         * @param builder     expression builder {@link ExpressionBuilder}
         * @param transformer transformer {@link Transformer}
         * @param delimiter   delimiter
         * @author: Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder(ExpressionBuilder builder, Transformer transformer, String delimiter) {
            this.transformer = transformer == null ? ExpressionBuilder::append : transformer;
            this.delimiter = delimiter;
            this.builder = builder;
        }

        /**
         * Return a new builder {@link ListBuilder} with the specified transform {@link Transformer}
         *
         * @param transformer transformer {@link Transformer}
         * @return list Expressible Object Builder {@link ListBuilder}
         * @author Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder transformedBy(Transformer transformer) {
            return new ListBuilder(builder, transformer, delimiter);
        }

        /**
         * Return a new builder {@link ListBuilder} with the specified delimiter
         *
         * @param delimiter delimiter
         * @return list Expressible Object Builder {@link ListBuilder}
         * @author Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder delimitedBy(String delimiter) {
            return new ListBuilder(builder, transformer, delimiter);
        }

        /**
         * Traverse the Iterator to append its value to the list builder {@link ListBuilder}
         *
         * @param iterable iterable
         * @return list Expressible Object Builder {@link ListBuilder}
         * @author Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder of(Iterable<?> iterable) {
            Iterator<?> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                if (first) {
                    first = false;
                } else {
                    builder.append(delimiter);
                }
                transformer.apply(builder, iterator.next());
            }
            return this;
        }

        /**
         * Append object to the builder
         *
         * @param object
         * @return expression builder {@link ExpressionBuilder}
         * @author Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder append(Object object) {
            builder.append(object);
            return this;
        }

        /**
         * Traverse the Iterator to append its value to the list builder {@link ListBuilder}
         *
         * @param iterable1 iterable
         * @param iterable2 iterable
         * @return list Expressible Object Builder {@link ListBuilder}
         * @author Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder of(Iterable<?> iterable1, Iterable<?> iterable2) {
            of(iterable1);
            of(iterable2);
            return this;
        }

        /**
         * Traverse the Iterator to append its value to the list builder {@link ListBuilder}
         *
         * @param iterable1 iterable
         * @param iterable2 iterable
         * @param iterable3 iterable
         * @return list Expressible Object Builder {@link ListBuilder}
         * @author Wang Wei
         * @time: 2023-06-10
         */
        public ListBuilder of(Iterable<?> iterable1, Iterable<?> iterable2, Iterable<?> iterable3) {
            of(iterable1);
            of(iterable2);
            of(iterable3);
            return this;
        }


    }

}
