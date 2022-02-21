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
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link BiConsumer
 * java.util.function.BiConsumer} which declares checked exception.
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 *
 * @since 4.0
 */
@FunctionalInterface
public interface BiConsumerEx<T, U> extends BiConsumer<T, U>, Serializable {

    /**
     * Exception-declaring version of {@link BiConsumer#accept}.
     * @throws Exception in case of any exceptional case
     */
    void acceptEx(T t, U u) throws Exception;

    @Override
    default void accept(T t, U u) {
        try {
            acceptEx(t, u);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of
     * {@link BiConsumer#andThen(BiConsumer) java.util.function.BiConsumer#andThen(BiConsumer)}.
     */
    default BiConsumerEx<T, U> andThen(BiConsumerEx<? super T, ? super U> after) {
        checkNotNull(after, "after");
        return (left, right) -> {
            accept(left, right);
            after.accept(left, right);
        };
    }
}
