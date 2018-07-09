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
import java.util.function.DoubleConsumer;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link DoubleConsumer
 * java.util.function.DoubleConsumer} which declares checked exception.
 */
@FunctionalInterface
public interface DistributedDoubleConsumer extends DoubleConsumer, Serializable {

    /**
     * Exception-declaring version of {@link DoubleConsumer#accept}.
     */
    void acceptEx(double value) throws Exception;

    @Override
    default void accept(double value) {
        try {
            acceptEx(value);
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    /**
     * {@code Serializable} variant of {@link DoubleConsumer#andThen(DoubleConsumer)
     * java.util.function.DoubleConsumer#andThen(DoubleConsumer)}.
     */
    default DistributedDoubleConsumer andThen(DistributedDoubleConsumer after) {
        checkNotNull(after, "after");
        return n -> {
            accept(n);
            after.accept(n);
        };
    }
}
