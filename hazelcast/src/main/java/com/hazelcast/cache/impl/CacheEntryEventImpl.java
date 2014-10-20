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

import com.hazelcast.cache.ICache;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;

/**
 * CacheEntryEvent implementation is the actual event object received by sub-interfaces of
 * {@link javax.cache.event.CacheEntryListener}.
 * <p>This implementation will provide source cache, the event type, key, value old value old value and availability</p>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 *
 * @see javax.cache.event.CacheEntryEvent
 * @see javax.cache.event.CacheEntryCreatedListener#onCreated(Iterable)
 * @see javax.cache.event.CacheEntryUpdatedListener#onUpdated(Iterable)
 * @see javax.cache.event.CacheEntryRemovedListener#onRemoved(Iterable)
 * @see javax.cache.event.CacheEntryExpiredListener#onExpired(Iterable)
 */
public class CacheEntryEventImpl<K, V>
        extends CacheEntryEvent<K, V> {

    private final K key;
    private final V newValue;
    private final V oldValue;

    public CacheEntryEventImpl(ICache<K, V> source, EventType eventType, K key, V newValue, V oldValue) {
        super(source, eventType);
        this.key = key;
        this.newValue = newValue;
        this.oldValue = oldValue;
    }

    @Override
    public V getOldValue() {
        return oldValue;
    }

    @Override
    public boolean isOldValueAvailable() {
        return oldValue != null;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return newValue;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

}
