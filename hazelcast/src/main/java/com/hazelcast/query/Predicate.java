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

package com.hazelcast.query;

import com.hazelcast.internal.serialization.BinaryInterface;

import java.io.Serializable;
import java.util.Map;

/**
 * Represents a map entry predicate. Implementations of this interface are basic
 * building blocks for performing queries on map entries.
 * <p>
 * Implementations <i>must</i> be thread-safe, since the {@link #apply}
 * method may be called by multiple threads concurrently.
 *
 * @param <K> the type of keys the predicate operates on.
 * @param <V> the type of values the predicate operates on.
 */
@BinaryInterface
@FunctionalInterface
public interface Predicate<K, V> extends Serializable {

    /**
     * Applies this predicate to the given map entry.
     * <p>
     * Implementations <i>must</i> be thread-safe, since this method may be
     * called by multiple threads concurrently.
     *
     * @param mapEntry the map entry to apply this predicate to.
     * @return {@code true} if the given map entry matches this predicate,
     * {@code false} otherwise.
     */
    boolean apply(Map.Entry<K, V> mapEntry);

}
