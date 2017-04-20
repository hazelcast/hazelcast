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

package com.hazelcast.jet;

import java.util.Map;

/**
 * Factory methods for several common distributed functions.
 */
public final class DistributedFunctions {

    private DistributedFunctions() {
    }

    /**
     * Synonym for {@link Distributed.Function#identity}, to be used as a
     * projection function (e.g., key extractor).
     */
    public static <T> Distributed.Function<T, T> wholeItem() {
        return Distributed.Function.identity();
    }

    /**
     * Returns a function that extracts the key of a {@link Map.Entry}.
     *
     * @param <K> type of entry's key
     */
    public static <K> Distributed.Function<Map.Entry<K, ?>, K> entryKey() {
        return Map.Entry::getKey;
    }

    /**
     * Returns a function that extracts the value of a {@link Map.Entry}.
     *
     * @param <V> type of entry's value
     */
    public static <V> Distributed.Function<Map.Entry<?, V>, V> entryValue() {
        return Map.Entry::getValue;
    }

    /**
     * Returns a consumer that does nothing with the argument.
     */
    public static <T> Distributed.Consumer<T> noopConsumer() {
        return t -> {
        };
    }
}
