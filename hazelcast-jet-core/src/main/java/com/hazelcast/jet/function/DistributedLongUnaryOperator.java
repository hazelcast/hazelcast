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
import java.util.function.LongUnaryOperator;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link LongUnaryOperator
 * java.util.function.LongUnaryOperator} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedLongUnaryOperator extends LongUnaryOperator, Serializable {

    /**
     * Exception-declaring version of {@link LongUnaryOperator#applyAsLong}.
     */
    long applyAsLongEx(long operand) throws Exception;

    @Override
    default long applyAsLong(long operand) {
        try {
            return applyAsLongEx(operand);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link LongUnaryOperator#identity()
     * java.util.function.LongUnaryOperator#identity()}.
     */
    static DistributedLongUnaryOperator identity() {
        return n -> n;
    }

    /**
     * {@code Serializable} variant of {@link
     * LongUnaryOperator#compose(LongUnaryOperator)
     * java.util.function.LongUnaryOperator#compose(LongUnaryOperator)}.
     */
    default DistributedLongUnaryOperator compose(DistributedLongUnaryOperator before) {
        checkNotNull(before, "before");
        return n -> applyAsLong(before.applyAsLong(n));
    }

    /**
     * {@code Serializable} variant of {@link
     * LongUnaryOperator#andThen(LongUnaryOperator)
     * java.util.function.LongUnaryOperator#andThen(LongUnaryOperator)}.
     */
    default DistributedLongUnaryOperator andThen(DistributedLongUnaryOperator after) {
        checkNotNull(after, "after");
        return n -> after.applyAsLong(applyAsLong(n));
    }
}
