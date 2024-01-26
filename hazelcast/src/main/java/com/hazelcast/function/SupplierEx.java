/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@code Serializable} variant of {@link Supplier java.util.function.Supplier}
 * which declares checked exception.
 *
 * @param <T> the type of results supplied by this supplier
 *
 * @since 4.0
 */
@FunctionalInterface
public interface SupplierEx<T> extends Supplier<T>, Serializable, SecuredFunction {

    /**
     * Exception-declaring version of {@link Supplier#get}.
     * @throws Exception in case of any exceptional case
     */
    T getEx() throws Exception;

    @Override
    default T get() {
        try {
            return getEx();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} analogue of {@link Function#andThen(Function)
     * java.util.function.Function#andThen(Function)}.
     * @param <V> the type of output of the {@code after} function, and of the
     *           composed function
     * @since 5.4
     */
    default <V> SupplierEx<V> andThen(FunctionEx<? super T, ? extends V> after) {
        return new FunctionsImpl.ComposedSupplierEx(this, after);
    }
}
