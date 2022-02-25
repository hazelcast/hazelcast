/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.security.impl.function.SecuredFunction;

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
public interface FunctionEx<T, R> extends Function<T, R>, Serializable, SecuredFunction {

    /**
     * Exception-declaring version of {@link Function#apply}.
     * @throws Exception in case of any exceptional case
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
     * @param <T> the type of the input and output objects to the function
     */
    @SuppressWarnings("unchecked")
    static <T> FunctionEx<T, T> identity() {
        return Util.Identity.INSTANCE;
    }

    /**
     * {@code Serializable} variant of {@link Function#compose(Function)
     * java.util.function.Function#compose(Function)}.
     * @param <V> the type of input to the {@code before} function, and to the
     *           composed function
     */
    default <V> FunctionEx<V, R> compose(FunctionEx<? super V, ? extends T> before) {
        checkNotNull(before, "before");
        return v -> apply(before.apply(v));
    }

    /**
     * {@code Serializable} variant of {@link Function#andThen(Function)
     * java.util.function.Function#andThen(Function)}.
     * @param <V> the type of output of the {@code after} function, and of the
     *           composed function
     */
    default <V> FunctionEx<T, V> andThen(FunctionEx<? super R, ? extends V> after) {
        checkNotNull(after, "after");
        return t -> after.apply(apply(t));
    }
}
