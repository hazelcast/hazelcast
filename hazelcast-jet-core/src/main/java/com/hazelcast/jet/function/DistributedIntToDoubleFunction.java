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
import java.util.function.IntToDoubleFunction;

/**
 * {@code Serializable} variant of {@link IntToDoubleFunction
 * java.util.function.IntToDoubleFunction} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedIntToDoubleFunction extends IntToDoubleFunction, Serializable {

    /**
     * Exception-declaring version of {@link IntToDoubleFunction#applyAsDouble}.
     */
    double applyAsDoubleEx(int value) throws Exception;

    @Override
    default double applyAsDouble(int value) {
        try {
            return applyAsDoubleEx(value);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
