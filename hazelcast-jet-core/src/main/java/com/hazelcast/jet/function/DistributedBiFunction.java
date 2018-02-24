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

import java.io.Serializable;
import java.util.function.BiFunction;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link
 * BiFunction java.util.function.BiFunction}.
 */
@FunctionalInterface
public interface DistributedBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * BiFunction#andThen(java.util.function.Function)
     * java.util.function.BiFunction#andThen(Function)}.
     */
    default <V> DistributedBiFunction<T, U, V> andThen(DistributedFunction<? super R, ? extends V> after) {
        checkNotNull(after, "after");
        return (t, u) -> after.apply(apply(t, u));
    }
}
