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
import java.util.function.DoublePredicate;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link DoublePredicate
 * java.util.function.DoublePredicate}.
 */
@FunctionalInterface
public interface DistributedDoublePredicate extends DoublePredicate, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * DoublePredicate#and(DoublePredicate)
     * java.util.function.DoublePredicate#and(DoublePredicate)}.
     */
    default DistributedDoublePredicate and(DistributedDoublePredicate other) {
        checkNotNull(other, "other");
        return (value) -> test(value) && other.test(value);
    }

    /**
     * {@code Serializable} variant of {@link DoublePredicate#negate()}.
     */
    @Override
    default DistributedDoublePredicate negate() {
        return v -> !test(v);
    }

    /**
     * {@code Serializable} variant of {@link
     * DoublePredicate#or(DoublePredicate)
     * java.util.function.DoublePredicate#or(DoublePredicate)}.
     */
    default DistributedDoublePredicate or(DistributedDoublePredicate other) {
        checkNotNull(other, "other");
        return (value) -> test(value) || other.test(value);
    }
}
