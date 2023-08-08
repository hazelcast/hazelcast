/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Represents a penta-arity function that accepts
 * five arguments and produces a result.
 *
 * @since Jet 3.0
 */
@FunctionalInterface
public interface PentaFunction<T0, T1, T2, T3, T4, R> extends Serializable {

    /**
     * Exception-declaring variant of {@link PentaFunction#apply}.
     */
    R applyEx(T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) throws Exception;

    /**
     * Applies this function to the given arguments.
     *
     * @param t0 the first argument
     * @param t1 the second argument
     * @param t2 the third argument
     * @param t3 the fourth argument
     * @return the function result
     */
    default R apply(T0 t0, T1 t1, T2 t2, T3 t3, T4 t4) {
        try {
            return applyEx(t0, t1, t2, t3, t4);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
