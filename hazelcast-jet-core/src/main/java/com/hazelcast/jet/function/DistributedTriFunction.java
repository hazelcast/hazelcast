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

import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.Serializable;

/**
 * {@code Serializable} variant of {@link TriFunction} which declares checked
 * exception.
 */
@FunctionalInterface
public interface DistributedTriFunction<T0, T1, T2, R> extends TriFunction<T0, T1, T2, R>, Serializable {

    /**
     * Exception-declaring variant of {@link TriFunction#apply}.
     */
    R applyEx(T0 t0, T1 t1, T2 t2) throws Exception;

    @Override
    default R apply(T0 t0, T1 t1, T2 t2) {
        try {
            return applyEx(t0, t1, t2);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
