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
import com.hazelcast.security.impl.function.SecuredFunction;

import java.io.Serializable;
import java.util.function.BiFunction;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link BiFunction
 * java.util.function.BiFunction} which declares checked exception.
 *
 * @param <T> the type of the first argument to the function
 * @param <U> the type of the second argument to the function
 * @param <R> the type of the result of the function
 *
 * @since 4.0
 */
@FunctionalInterface
public interface BiFunctionEx<T, U, R> extends BiFunction<T, U, R>, Serializable, SecuredFunction {

    /**
     * Exception-declaring version of {@link BiFunction#apply}.
     * @throws Exception in case of any exceptional case
     */
    R applyEx(T t, U u) throws Exception;

    @Override
    default R apply(T t, U u) {
        try {
            return applyEx(t, u);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link
     * BiFunction#andThen(java.util.function.Function)
     * java.util.function.BiFunction#andThen(Function)}.
     * @param <V> the type of output of the {@code after} function, and of the
     *           composed function
     */
    default <V> BiFunctionEx<T, U, V> andThen(FunctionEx<? super R, ? extends V> after) {
        checkNotNull(after, "after");
        return (t, u) -> after.apply(apply(t, u));
    }
}
