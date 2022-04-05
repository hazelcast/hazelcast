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

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Factory methods for several common functions.
 *
 * @since 4.0
 */
public final class Functions {

    private Functions() {
    }

    /**
     * Synonym for {@link FunctionEx#identity}, to be used as a
     * projection function (e.g., key extractor).
     * @param <T> the type of the input and output objects to the function
     */
    @Nonnull
    public static <T> FunctionEx<T, T> wholeItem() {
        return FunctionEx.identity();
    }

    /**
     * Returns a function that extracts the key of a {@link Entry}.
     *
     * @param <K> type of entry's key
     */
    @Nonnull
    public static <K> FunctionEx<Entry<K, ?>, K> entryKey() {
        return Entry::getKey;
    }

    /**
     * Returns a function that extracts the value of a {@link Entry}.
     *
     * @param <V> type of entry's value
     */
    @Nonnull
    public static <V> FunctionEx<Entry<?, V>, V> entryValue() {
        return Entry::getValue;
    }
}
