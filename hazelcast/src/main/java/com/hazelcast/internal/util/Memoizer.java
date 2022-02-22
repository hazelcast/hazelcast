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

package com.hazelcast.internal.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Thread safe cache for memoizing the calculation for a specific key.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class Memoizer<K, V> {
    /** Null object to be kept as values which are {@code null} */
    public static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<K, V> cache = new ConcurrentHashMap<>();

    /** Mutex factory for caching the values in {@link #cache} */
    private final ContextMutexFactory cacheMutexFactory = new ContextMutexFactory();

    /** The function for retrieving the value for a specific key */
    private final ConstructorFunction<K, V> constructorFunction;

    /**
     * Constructs the memoizer.
     *
     * @param calculationFunction the function for retrieving the value for a specific key
     */
    public Memoizer(ConstructorFunction<K, V> calculationFunction) {
        this.constructorFunction = calculationFunction;
    }

    /**
     * Returns the value for the {@code key}, calculating it in the process if
     * necessary.
     */
    public V getOrCalculate(K key) {
        final V value = getOrPutSynchronized(cache, key, cacheMutexFactory, constructorFunction);
        return value == NULL_OBJECT ? null : value;
    }

    /**
     * Removes the entry associated with the given {@code key}
     */
    public void remove(K key) {
        cache.remove(key);
    }
}
