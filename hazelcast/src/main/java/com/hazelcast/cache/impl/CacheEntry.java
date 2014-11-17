/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl;

import javax.cache.Cache;

/**
 * Simple  {@link javax.cache.Cache.Entry} implementation for wrapping a "key,value" pair.
 *
 * <p>This implementation is used by {@link javax.cache.integration.CacheWriter}s and
 * {@link javax.cache.Cache#iterator()}.</p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see javax.cache.integration.CacheWriter
 * @see javax.cache.Cache#iterator()
 */
public class CacheEntry<K, V>
        implements Cache.Entry<K, V> {
    private final K key;
    private V value;

    public CacheEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

}
