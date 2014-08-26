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

package com.hazelcast.cache;

import com.hazelcast.spi.NodeEngine;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.util.HashSet;

/**
 * Adapter for EventListener.
 *
 * @param <K>
 * @param <V>
 */
public class CacheEventListenerAdaptor<K, V> {

    private CacheEntryCreatedListener cacheEntryCreatedListener;
    private CacheEntryRemovedListener cacheEntryRemovedListener;
    private CacheEntryUpdatedListener cacheEntryUpdatedListener;
    private CacheEntryExpiredListener cacheEntryExpiredListener;

    private transient ICache<K, V> source;

    public CacheEventListenerAdaptor(ICache<K, V> source, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        this.source = source;
        final CacheEntryListener<? super K, ? super V> cacheEntryListener = cacheEntryListenerConfiguration
                .getCacheEntryListenerFactory().create();
        if (cacheEntryListener instanceof CacheEntryCreatedListener) {
            this.cacheEntryCreatedListener = (CacheEntryCreatedListener) cacheEntryListener;
        } else {
            this.cacheEntryCreatedListener = null;
        }
        if (cacheEntryListener instanceof CacheEntryRemovedListener) {
            this.cacheEntryRemovedListener = (CacheEntryRemovedListener) cacheEntryListener;
        } else {
            this.cacheEntryRemovedListener = null;
        }
        if (cacheEntryListener instanceof CacheEntryUpdatedListener) {
            this.cacheEntryUpdatedListener = (CacheEntryUpdatedListener) cacheEntryListener;
        } else {
            this.cacheEntryUpdatedListener = null;
        }
        if (cacheEntryListener instanceof CacheEntryExpiredListener) {
            this.cacheEntryExpiredListener = (CacheEntryExpiredListener) cacheEntryListener;
        } else {
            this.cacheEntryExpiredListener = null;
        }
    }

    public void handleEvent(NodeEngine nodeEngine, String cacheName, EventType eventType, K key, V newValue, V oldValue) {
        if (source == null) {
            //  this.source = nodeEngine.getHazelcastInstance().getDistributedObject(CacheService.SERVICE_NAME, cacheName);
        }
        final CacheEntryEventImpl<K, V> event = new CacheEntryEventImpl<K, V>(source, eventType, key, newValue, oldValue);
        switch (eventType) {
            case CREATED:
                if (this.cacheEntryCreatedListener != null) {
                    this.cacheEntryCreatedListener.onCreated(createEventWrapper(event));
                }
                break;
            case UPDATED:
                if (this.cacheEntryUpdatedListener != null) {
                    this.cacheEntryUpdatedListener.onUpdated(createEventWrapper(event));
                }
                break;
            case REMOVED:
                if (this.cacheEntryRemovedListener != null) {
                    this.cacheEntryRemovedListener.onRemoved(createEventWrapper(event));
                }
                break;
            case EXPIRED:
                if (this.cacheEntryExpiredListener != null) {
                    this.cacheEntryExpiredListener.onExpired(createEventWrapper(event));
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid event type: " + eventType.name());
        }
    }

    private Iterable<CacheEntryEvent<? extends K, ? extends V>> createEventWrapper(CacheEntryEvent<K, V> event) {
        HashSet<CacheEntryEvent<? extends K, ? extends V>> evt = new HashSet<CacheEntryEvent<? extends K, ? extends V>>();
        evt.add(event);
        return evt;
    }

}
