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
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Consumer java.util.function.Consumer}
 * which declares checked exception.
 *
 * @param <T> the type of the input to the operation
 *
 * @since 4.0
 */
@FunctionalInterface
public interface ConsumerEx<T> extends Consumer<T>, Serializable, SecuredFunction {

    /**
     * Exception-declaring version of {@link Consumer#accept}
     * @throws Exception in case of any exceptional case
     */
    void acceptEx(T t) throws Exception;

    @Override
    default void accept(T t) {
        try {
            acceptEx(t);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link Consumer#andThen(Consumer)
     * java.util.function.Consumer#andThen(Consumer)}.
     */
    default ConsumerEx<T> andThen(ConsumerEx<? super T> after) {
        checkNotNull(after, "after");
        return t -> {
            accept(t);
            after.accept(t);
        };
    }

    /**
     * Returns a consumer that does nothing.
     * @param <T> the consumer input type
     */
    static <T> ConsumerEx<T> noop() {
        return x -> {
        };
    }
}
