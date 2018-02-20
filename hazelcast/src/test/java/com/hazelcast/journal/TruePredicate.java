package com.hazelcast.journal;

import com.hazelcast.util.function.Predicate;

import java.io.Serializable;

/**
 * True predicate always returning {@code true}.
 *
 * @param <T> predicate argument type
 */
class TruePredicate<T> implements Predicate<T>, Serializable {
    @Override
    public boolean test(T t) {
        return true;
    }
}