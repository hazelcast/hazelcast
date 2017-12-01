/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.BinaryOperator;

/**
 * {@code Serializable} variant of {@link
 * BinaryOperator java.util.function.BinaryOperator}.
 */
@FunctionalInterface
public interface DistributedBinaryOperator<T> extends BinaryOperator<T>, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * BinaryOperator#minBy(Comparator) java.util.function.BinaryOperator#minBy(Comparator)}.
     */
    static <T> DistributedBinaryOperator<T> minBy(java.util.Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator);
        return (a, b) -> comparator.compare(a, b) <= 0 ? a : b;
    }

    /**
     * {@code Serializable} variant of {@link
     * BinaryOperator#maxBy(Comparator) java.util.function.BinaryOperator#maxBy(Comparator)}.
     */
    static <T> DistributedBinaryOperator<T> maxBy(java.util.Comparator<? super T> comparator) {
        Objects.requireNonNull(comparator);
        return (a, b) -> comparator.compare(a, b) >= 0 ? a : b;
    }
}
