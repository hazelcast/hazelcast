/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.function.LongPredicate;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link LongPredicate
 * java.util.function.LongPredicate} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedLongPredicate extends LongPredicate, Serializable {

    /**
     * Exception-declaring version of {@link LongPredicate#test}.
     */
    boolean testEx(long value) throws Exception;

    @Override
    default boolean test(long value) {
        try {
            return testEx(value);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * LongPredicate#and(LongPredicate)
     * java.util.function.LongPredicate#and(LongPredicate)}.
     */
    default DistributedLongPredicate and(DistributedLongPredicate other) {
        checkNotNull(other, "other");
        return n -> test(n) && other.test(n);
    }

    /**
     * {@code Serializable} variant of {@link LongPredicate#negate()}.
     */
    @Override
    default DistributedLongPredicate negate() {
        return (value) -> !test(value);
    }

    /**
     * {@code Serializable} variant of {@link
     * LongPredicate#or(LongPredicate)
     * java.util.function.LongPredicate#or(LongPredicate)}.
     */
    default DistributedLongPredicate or(DistributedLongPredicate other) {
        checkNotNull(other, "other");
        return n -> test(n) || other.test(n);
    }
}
