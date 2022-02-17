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

import java.io.Serializable;
import java.util.function.BiPredicate;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link BiPredicate
 * java.util.function.BiPredicate} which declares checked exception.
 *
 * @param <T> the type of the first argument to the predicate
 * @param <U> the type of the second argument the predicate
 *
 * @since 4.0
 */
@FunctionalInterface
public interface BiPredicateEx<T, U> extends BiPredicate<T, U>, Serializable {

    /**
     * Exception-declaring version of {@link BiPredicate#test}.
     * @throws Exception in case of any exceptional case
     */
    boolean testEx(T t, U u) throws Exception;

    @Override
    default boolean test(T t, U u) {
        try {
            return testEx(t, u);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * BiPredicate#and(BiPredicate) java.util.function.BiPredicate#and(BiPredicate)}.
     */
    default BiPredicateEx<T, U> and(BiPredicateEx<? super T, ? super U> other) {
        checkNotNull(other, "other");
        return (T t, U u) -> test(t, u) && other.test(t, u);
    }

    /**
     * {@code Serializable} variant of {@link
     * BiPredicate#negate() java.util.function.BiPredicate#negate()}.
     */
    @Override
    default BiPredicateEx<T, U> negate() {
        return (T t, U u) -> !test(t, u);
    }

    /**
     * {@code Serializable} variant of {@link
     * BiPredicate#or(BiPredicate) java.util.function.BiPredicate#or(BiPredicate)}.
     */
    default BiPredicateEx<T, U> or(BiPredicateEx<? super T, ? super U> other) {
        checkNotNull(other, "other");
        return (T t, U u) -> test(t, u) || other.test(t, u);
    }
}
