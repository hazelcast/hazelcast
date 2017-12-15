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
import java.util.function.Function;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Function
 * java.util.function.Function}.
 */
@FunctionalInterface
public interface DistributedFunction<T, R> extends Function<T, R>, Serializable {

    /**
     * {@code Serializable} variant of {@link Function#identity()
     * java.util.function.Function#identity()}.
     */
    static <T> DistributedFunction<T, T> identity() {
        return t -> t;
    }

    /**
     * {@code Serializable} variant of {@link Function#compose(Function)
     * java.util.function.Function#compose(Function)}.
     */
    default <V> DistributedFunction<V, R> compose(DistributedFunction<? super V, ? extends T> before) {
        checkNotNull(before, "before");
        return v -> apply(before.apply(v));
    }

    /**
     * {@code Serializable} variant of {@link Function#andThen(Function)
     * java.util.function.Function#andThen(Function)}.
     */
    default <V> DistributedFunction<T, V> andThen(DistributedFunction<? super R, ? extends V> after) {
        checkNotNull(after, "after");
        return t -> after.apply(apply(t));
    }
}
