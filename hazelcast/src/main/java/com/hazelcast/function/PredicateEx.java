/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.function;

import com.hazelcast.internal.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Predicate
 * java.util.function.Predicate} which declares checked exception.
 *
 * @param <T> the type of the input to the predicate
 *
 * @since 4.0
 */
@FunctionalInterface
public interface PredicateEx<T> extends Predicate<T>, Serializable {

    /**
     * Exception-declaring version of {@link Predicate#test}.
     * @throws Exception in case of any exceptional case
     */
    boolean testEx(T t) throws Exception;

    @Override
    default boolean test(T t) {
        try {
            return testEx(t);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * Returns a predicate that always evaluates to {@code true}.
     * @param <T> the type of the input to the predicate
     */
    @Nonnull
    static <T> PredicateEx<T> alwaysTrue() {
        return t -> true;
    }

    /**
     * Returns a predicate that always evaluates to {@code false}.
     * @param <T> the type of the input to the predicate
     */
    @Nonnull
    static <T> PredicateEx<T> alwaysFalse() {
        return t -> false;
    }

    /**
     * {@code Serializable} variant of
     * @param <T> the type of the input to the predicate
     * {@link Predicate#isEqual(Object) java.util.function.Predicate#isEqual(Object)}.
     */
    @Nonnull
    static <T> PredicateEx<T> isEqual(Object other) {
        return other == null ? Objects::isNull : other::equals;
    }

    /**
     * {@code Serializable} variant of
     * {@link Predicate#and(Predicate) java.util.function.Predicate#and(Predicate)}.
     */
    @Nonnull
    default PredicateEx<T> and(PredicateEx<? super T> other) {
        checkNotNull(other, "other");
        return t -> test(t) && other.test(t);
    }

    /**
     * {@code Serializable} variant of
     * {@link Predicate#negate()}.
     */
    @Nonnull
    @Override
    default PredicateEx<T> negate() {
        return t -> !test(t);
    }

    /**
     * {@code Serializable} variant of
     * {@link Predicate#or(Predicate) java.util.function.Predicate#or(Predicate)}.
     */
    @Nonnull
    default PredicateEx<T> or(PredicateEx<? super T> other) {
        checkNotNull(other, "other");
        return t -> test(t) || other.test(t);
    }
}
