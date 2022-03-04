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

import java.io.Serializable;
import java.util.function.ToIntFunction;

/**
 * {@code Serializable} variant of {@link ToIntFunction
 * java.util.function.ToIntFunction} which declares checked exception.
 *
 * @param <T> the type of the input to the function
 *
 * @since 4.0
 */
@FunctionalInterface
public interface ToIntFunctionEx<T> extends ToIntFunction<T>, Serializable {

    /**
     * Exception-declaring version of {@link ToIntFunction#applyAsInt}.
     * @throws Exception in case of any exceptional case
     */
    int applyAsIntEx(T value) throws Exception;

    @Override
    default int applyAsInt(T value) {
        try {
            return applyAsIntEx(value);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
