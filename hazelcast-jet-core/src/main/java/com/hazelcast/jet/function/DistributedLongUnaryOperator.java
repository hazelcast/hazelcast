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
import java.util.Objects;
import java.util.function.LongUnaryOperator;

/**
 * {@code Serializable} variant of {@link LongUnaryOperator
 * java.util.function.LongUnaryOperator}.
 */
@FunctionalInterface
public interface DistributedLongUnaryOperator extends LongUnaryOperator, Serializable {

    /**
     * {@code Serializable} variant of {@link LongUnaryOperator#identity()
     * java.util.function.LongUnaryOperator#identity()}.
     */
    static DistributedLongUnaryOperator identity() {
        return t -> t;
    }

    /**
     * {@code Serializable} variant of {@link
     * LongUnaryOperator#compose(LongUnaryOperator)
     * java.util.function.LongUnaryOperator#compose(LongUnaryOperator)}.
     */
    default DistributedLongUnaryOperator compose(DistributedLongUnaryOperator before) {
        Objects.requireNonNull(before);
        return (long v) -> applyAsLong(before.applyAsLong(v));
    }

    /**
     * {@code Serializable} variant of {@link
     * LongUnaryOperator#andThen(LongUnaryOperator)
     * java.util.function.LongUnaryOperator#andThen(LongUnaryOperator)}.
     */
    default DistributedLongUnaryOperator andThen(DistributedLongUnaryOperator after) {
        Objects.requireNonNull(after);
        return (long t) -> after.applyAsLong(applyAsLong(t));
    }
}
