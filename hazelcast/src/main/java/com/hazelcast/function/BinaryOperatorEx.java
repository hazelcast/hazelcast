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
import java.util.Comparator;
import java.util.function.BinaryOperator;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link BinaryOperator
 * java.util.function.BinaryOperator} which declares checked exception.
 *
 * @param <T> the type of the operands and result of the operator
 *
 * @since 4.0
 */
@FunctionalInterface
public interface BinaryOperatorEx<T> extends BinaryOperator<T>, Serializable {

    /**
     * Exception-declaring version of {@link BinaryOperator#apply}.
     * @throws Exception in case of any exceptional case
     */
    T applyEx(T t1, T t2) throws Exception;

    @Override
    default T apply(T t1, T t2) {
        try {
            return applyEx(t1, t2);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * BinaryOperator#minBy(Comparator) java.util.function.BinaryOperator#minBy(Comparator)}.
     * @param <T> the type of the input arguments of the comparator
     */
    static <T> BinaryOperatorEx<T> minBy(Comparator<? super T> comparator) {
        checkNotNull(comparator, "comparator");
        return (l, r) -> comparator.compare(l, r) <= 0 ? l : r;
    }

    /**
     * {@code Serializable} variant of {@link
     * BinaryOperator#maxBy(Comparator) java.util.function.BinaryOperator#maxBy(Comparator)}.
     * @param <T> the type of the input arguments of the comparator
     */
    static <T> BinaryOperatorEx<T> maxBy(Comparator<? super T> comparator) {
        checkNotNull(comparator, "comparator");
        return (l, r) -> comparator.compare(l, r) >= 0 ? l : r;
    }
}
