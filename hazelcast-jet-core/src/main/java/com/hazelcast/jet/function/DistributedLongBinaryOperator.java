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
import java.util.function.LongBinaryOperator;

/**
 * {@code Serializable} variant of {@link LongBinaryOperator
 * java.util.function.LongBinaryOperator} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedLongBinaryOperator extends LongBinaryOperator, Serializable {

    /**
     * Exception-declaring version of {@link LongBinaryOperator#applyAsLong}.
     */
    long applyAsLongEx(long left, long right) throws Exception;

    @Override
    default long applyAsLong(long left, long right) {
        try {
            return applyAsLongEx(left, right);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
