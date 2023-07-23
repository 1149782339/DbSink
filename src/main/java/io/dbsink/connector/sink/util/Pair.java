package io.dbsink.connector.sink.util;

import java.util.Objects;

/**
 * A simple generic class representing a pair of values.
 *
 * @param <K> the type of the first value
 * @param <V> the type of the second value
 * @author Wang Wei
 * @time: 2023-07-16
 */
public class Pair<K, V> {
    private K first;
    private V second;

    private Pair(K first, V second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Returns the first value of the pair.
     *
     * @return the first value
     */
    public K getFirst() {
        return first;
    }

    /**
     * Returns the second value of the pair.
     *
     * @return the second value
     */
    public V getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) && Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    /**
     * Constructs a new Pair object with the specified values.
     *
     * @param first  the first value of the pair
     * @param second the second value of the pair
     */
    public static <K, V> Pair of(K first, V second) {
        return new Pair<>(first, second);
    }
}
