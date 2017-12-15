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
import java.util.function.DoubleUnaryOperator;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link DoubleUnaryOperator
 * java.util.function.DoubleUnaryOperator}.
 */
@FunctionalInterface
public interface DistributedDoubleUnaryOperator extends DoubleUnaryOperator, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * DoubleUnaryOperator#identity() java.util.function.DoubleUnaryOperator#identity()}.
     */
    static DistributedDoubleUnaryOperator identity() {
        return n -> n;
    }

    /**
     * {@code Serializable} variant of {@link
     * DoubleUnaryOperator#compose(DoubleUnaryOperator)
     * java.util.function.DoubleUnaryOperator#compose(DoubleUnaryOperator)}.
     */
    default DistributedDoubleUnaryOperator compose(DistributedDoubleUnaryOperator before) {
        checkNotNull(before, "before");
        return n -> applyAsDouble(before.applyAsDouble(n));
    }

    /**
     * {@code Serializable} variant of {@link
     * DoubleUnaryOperator#andThen(DoubleUnaryOperator)
     * java.util.function.DoubleUnaryOperator#andThen(DoubleUnaryOperator)}.
     */
    default DistributedDoubleUnaryOperator andThen(DistributedDoubleUnaryOperator after) {
        checkNotNull(after, "after");
        return n -> after.applyAsDouble(applyAsDouble(n));
    }
}
