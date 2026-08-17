/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import java.util.function.Function;

/**
 * A {@link Function} that declares checked exception.
 *
 * @param <T> the type of the argument to the function
 * @param <R> the type of the result of the function
 *
 * @since 5.7
 */
@FunctionalInterface
public interface ThrowingFunction<T, R> extends Function<T, R> {

    /**
     * Apply given items.
     * @see Function#apply(Object)
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
}
