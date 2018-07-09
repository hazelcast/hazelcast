/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Predicate
 * java.util.function.Predicate} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedPredicate<T> extends Predicate<T>, Serializable {

    /**
     * Exception-declaring version of {@link Predicate#test}.
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
     * {@code Serializable} variant of
     * {@link Predicate#isEqual(Object) java.util.function.Predicate#isEqual(Object)}.
     */
    static <T> DistributedPredicate<T> isEqual(Object other) {
        return null == other ? Objects::isNull : other::equals;
    }

    /**
     * {@code Serializable} variant of
     * {@link Predicate#and(Predicate) java.util.function.Predicate#and(Predicate)}.
     */
    default DistributedPredicate<T> and(DistributedPredicate<? super T> other) {
        checkNotNull(other, "other");
        return t -> test(t) && other.test(t);
    }

    /**
     * {@code Serializable} variant of
     * {@link Predicate#negate()}.
     */
    @Override
    default DistributedPredicate<T> negate() {
        return t -> !test(t);
    }

    /**
     * {@code Serializable} variant of
     * {@link Predicate#or(Predicate) java.util.function.Predicate#or(Predicate)}.
     */
    default DistributedPredicate<T> or(DistributedPredicate<? super T> other) {
        checkNotNull(other, "other");
        return t -> test(t) || other.test(t);
    }
}
