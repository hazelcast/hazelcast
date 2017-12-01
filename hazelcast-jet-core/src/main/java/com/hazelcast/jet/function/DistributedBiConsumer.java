/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * {@code Serializable} variant of {@link BiConsumer
 * java.util.function.BiConsumer}.
 */
@FunctionalInterface
public interface DistributedBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {

    /**
     * {@code Serializable} variant of
     * {@link BiConsumer#andThen(BiConsumer) java.util.function.BiConsumer#andThen(BiConsumer)}.
     */
    default DistributedBiConsumer<T, U> andThen(DistributedBiConsumer<? super T, ? super U> after) {
        Objects.requireNonNull(after);

        return (l, r) -> {
            accept(l, r);
            after.accept(l, r);
        };
    }
}
