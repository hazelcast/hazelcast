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

package com.hazelcast.function;

import com.hazelcast.internal.util.ExceptionUtil;

import java.io.Serializable;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Function java.util.function.Function}
 * which declares checked exception.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 *
 * @since 4.0
 */
@FunctionalInterface
public interface FunctionEx<T, R> extends Function<T, R>, Serializable {

    /**
     * Exception-declaring version of {@link Function#apply}.
     */
    R applyEx(T t) throws Exception;

    @Override
    default R apply(T t) {
        try {
            return applyEx(t);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link Function#identity()
     * java.util.function.Function#identity()}.
     */
    static <T> FunctionEx<T, T> identity() {
        return t -> t;
    }

    /**
     * {@code Serializable} variant of {@link Function#compose(Function)
     * java.util.function.Function#compose(Function)}.
     */
    default <V> FunctionEx<V, R> compose(FunctionEx<? super V, ? extends T> before) {
        checkNotNull(before, "before");
        return v -> apply(before.apply(v));
    }

    /**
     * {@code Serializable} variant of {@link Function#andThen(Function)
     * java.util.function.Function#andThen(Function)}.
     */
    default <V> FunctionEx<T, V> andThen(FunctionEx<? super R, ? extends V> after) {
        checkNotNull(after, "after");
        return t -> after.apply(apply(t));
    }
}
