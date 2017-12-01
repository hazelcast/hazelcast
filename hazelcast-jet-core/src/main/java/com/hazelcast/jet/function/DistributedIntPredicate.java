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
import java.util.function.IntPredicate;

/**
 * {@code Serializable} variant of {@link IntPredicate
 * java.util.function.IntPredicate}.
 */
@FunctionalInterface
public interface DistributedIntPredicate extends IntPredicate, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * IntPredicate#and(IntPredicate)
     * java.util.function.IntPredicate#and(IntPredicate)}.
     */
    default DistributedIntPredicate and(DistributedIntPredicate other) {
        Objects.requireNonNull(other);
        return (value) -> test(value) && other.test(value);
    }

    /**
     * {@code Serializable} variant of {@link IntPredicate#negate()}.
     */
    @Override
    default DistributedIntPredicate negate() {
        return (value) -> !test(value);
    }

    /**
     * {@code Serializable} variant of {@link
     * IntPredicate#or(IntPredicate)
     * java.util.function.IntPredicate#or(IntPredicate)}.
     */
    default DistributedIntPredicate or(DistributedIntPredicate other) {
        Objects.requireNonNull(other);
        return (value) -> test(value) || other.test(value);
    }
}
