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

package com.hazelcast.jet.function;


import com.hazelcast.jet.impl.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Represents a predicate which accepts three arguments. This
 * is the three-arity specialization of {@link Predicate}.
 *
 * @param <T> the type of the first argument to the predicate
 * @param <U> the type of the second argument to the predicate
 * @param <V> the type of the third argument to the predicate
 * @since Jet 3.0
 */
@FunctionalInterface
public interface TriPredicate<T, U, V> extends Serializable {

    /**
     * Exception-declaring version of {@link TriPredicate#test}.
     */
    boolean testEx(T t, U u, V v) throws Exception;

    /**
     * Evaluates this predicate with the given arguments.
     *
     * @param t the first argument
     * @param u the second argument
     * @param v the third argument
     * @return {@code true} if predicate evaluated to true, {@code false} otherwise
     */
    default boolean test(T t, U u, V v) {
        try {
            return testEx(t, u, v);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * Returns a composite predicate which evaluates the
     * equivalent of {@code this.test(t, u, v) && other.test(t, u, v)}.
     *
     */
    default TriPredicate<T, U, V> and(
            @Nonnull TriPredicate<? super T, ? super U, ? super V> other
    ) {
        checkNotNull(other, "other");
        return (t, u, v) -> test(t, u, v) && other.test(t, u, v);
    }

    /**
     * Returns a composite predicate which evaluates the
     * equivalent of {@code !this.test(t, u, v)}.
     */
    default TriPredicate<T, U, V> negate() {
        return (t, u, v) -> !test(t, u, v);
    }

    /**
     * Returns a composite predicate which evaluates the
     * equivalent of {@code this.test(t, u, v) || other.test(t, u, v)}.
     */
    default TriPredicate<T, U, V> or(
            @Nonnull TriPredicate<? super T, ? super U, ? super V> other
    ) {
        checkNotNull(other, "other");
        return (t, u, v) -> test(t, u, v) || other.test(t, u, v);
    }
}
