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

package com.hazelcast.cache.impl.merge.entry;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * An implementation of {@link com.hazelcast.cache.CacheEntryView}
 * for converting key and value to object when they are touched as lazy.
 */
public class LazyCacheEntryView<K, V>
        implements CacheEntryView<K, V> {

    private Object key;
    private Object value;
    private Object expiryPolicy;
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long accessHit;
    private SerializationService serializationService;

    public LazyCacheEntryView(Object key, Object value, long creationTime,
                              long expirationTime, long lastAccessTime, long accessHit, Object expiryPolicy) {
        // `null` `serializationService` means, use raw type without any conversion
        this(key, value, creationTime, expirationTime, lastAccessTime, accessHit, expiryPolicy, null);
    }

    public LazyCacheEntryView(Object key, Object value, long creationTime,
                              long expirationTime, long lastAccessTime, long accessHit, Object expiryPolicy,
                              SerializationService serializationService) {
        this.key = key;
        this.value = value;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.lastAccessTime = lastAccessTime;
        this.accessHit = accessHit;
        this.expiryPolicy = expiryPolicy;
        this.serializationService = serializationService;
    }

    @Override
    public K getKey() {
        // `null` `serializationService` means, use raw type without any conversion
        if (serializationService != null) {
            key = serializationService.toObject(key);
        }
        return (K) key;
    }

    @Override
    public V getValue() {
        // `null` `serializationService` means, use raw type without any conversion
        if (serializationService != null) {
            value = serializationService.toObject(value);
        }
        return (V) value;
    }

    @Override
    public Object getExpiryPolicy() {
        if (serializationService != null) {
            expiryPolicy = serializationService.toObject(expiryPolicy);
        }
        return expiryPolicy;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getHits() {
        return accessHit;
    }

}
