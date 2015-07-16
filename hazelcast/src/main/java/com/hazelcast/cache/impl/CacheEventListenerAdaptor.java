/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.ListenerWrapperEventFilter;
import com.hazelcast.spi.NotifiableEventListener;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;

/**
 * This implementation of {@link CacheEventListener} uses the adapter pattern for wrapping all cache event listener
 * types into a single listener.
 * <p>JCache has multiple {@link CacheEntryListener} sub-interfaces for each event type. This adapter
 * implementation delegates to the correct subtype using the event type.</p>
 * <p/>
 * <p>Another responsibility of this implementation is filtering events by using the already configured
 * event filters.</p>
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see javax.cache.event.CacheEntryCreatedListener
 * @see javax.cache.event.CacheEntryUpdatedListener
 * @see javax.cache.event.CacheEntryRemovedListener
 * @see javax.cache.event.CacheEntryExpiredListener
 * @see javax.cache.event.CacheEntryEventFilter
 */
@SuppressFBWarnings(value = "SE_NO_SERIALVERSIONID",
        justification = "Class is Serializable, but doesn't define serialVersionUID")
public class CacheEventListenerAdaptor<K, V>
        implements CacheEventListener,
                   CacheEntryListenerProvider<K, V>,
                   NotifiableEventListener<CacheService>,
                   ListenerWrapperEventFilter,
                   Serializable {

    private final transient CacheEntryListener<K, V> cacheEntryListener;
    private final transient CacheEntryCreatedListener cacheEntryCreatedListener;
    private final transient CacheEntryRemovedListener cacheEntryRemovedListener;
    private final transient CacheEntryUpdatedListener cacheEntryUpdatedListener;
    private final transient CacheEntryExpiredListener cacheEntryExpiredListener;
    private final transient CacheEntryEventFilter<? super K, ? super V> filter;
    private final boolean isOldValueRequired;

    private transient SerializationService serializationService;
    private transient ICache<K, V> source;

    public CacheEventListenerAdaptor(ICache<K, V> source,
                                     CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration,
                                     SerializationService serializationService) {
        this.source = source;
        this.serializationService = serializationService;

        final CacheEntryListener<? super K, ? super V> cacheEntryListener =
                cacheEntryListenerConfiguration.getCacheEntryListenerFactory().create();

        this.cacheEntryListener = (CacheEntryListener<K, V>) cacheEntryListener;
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
        final Factory<CacheEntryEventFilter<? super K, ? super V>> filterFactory =
                cacheEntryListenerConfiguration.getCacheEntryEventFilterFactory();
        if (filterFactory != null) {
            this.filter = filterFactory.create();
        } else {
            this.filter = null;
        }
        this.isOldValueRequired = cacheEntryListenerConfiguration.isOldValueRequired();
    }

    @Override
    public CacheEntryListener<K, V> getCacheEntryListener() {
        return cacheEntryListener;
    }

    @Override
    public void handleEvent(Object eventObject) {
        if (eventObject instanceof CacheEventSet) {
            CacheEventSet cacheEventSet = (CacheEventSet) eventObject;
            try {
                if (cacheEventSet.getEventType() != CacheEventType.COMPLETED) {
                    handleEvent(cacheEventSet.getEventType().getType(), cacheEventSet.getEvents());
                }
            } finally {
                ((CacheSyncListenerCompleter) source).countDownCompletionLatch(cacheEventSet.getCompletionId());
            }
        }
    }

    private void handleEvent(int type, Collection<CacheEventData> keys) {
        final Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvent = createCacheEntryEvent(keys);
        CacheEventType eventType = CacheEventType.getByType(type);
        switch (eventType) {
            case CREATED:
                if (this.cacheEntryCreatedListener != null) {
                    this.cacheEntryCreatedListener.onCreated(cacheEntryEvent);
                }
                break;
            case UPDATED:
                if (this.cacheEntryUpdatedListener != null) {
                    this.cacheEntryUpdatedListener.onUpdated(cacheEntryEvent);
                }
                break;
            case REMOVED:
                if (this.cacheEntryRemovedListener != null) {
                    this.cacheEntryRemovedListener.onRemoved(cacheEntryEvent);
                }
                break;
            case EXPIRED:
                if (this.cacheEntryExpiredListener != null) {
                    this.cacheEntryExpiredListener.onExpired(cacheEntryEvent);
                }
                break;
            default:
                throw new IllegalArgumentException("Invalid event type: " + eventType.name());
        }
    }

    private Iterable<CacheEntryEvent<? extends K, ? extends V>> createCacheEntryEvent(Collection<CacheEventData> keys) {
        HashSet<CacheEntryEvent<? extends K, ? extends V>> evt = new HashSet<CacheEntryEvent<? extends K, ? extends V>>();
        for (CacheEventData cacheEventData : keys) {
            final EventType eventType = CacheEventType.convertToEventType(cacheEventData.getCacheEventType());
            final K key = toObject(cacheEventData.getDataKey());
            final V newValue = toObject(cacheEventData.getDataValue());
            final V oldValue;
            if (isOldValueRequired) {
                oldValue = toObject(cacheEventData.getDataOldValue());
            } else {
                oldValue = null;
            }
            final CacheEntryEventImpl<K, V> event =
                    new CacheEntryEventImpl<K, V>(source, eventType, key, newValue, oldValue);
            if (filter == null || filter.evaluate(event)) {
                evt.add(event);
            }
        }
        return evt;
    }

    private <T> T toObject(Data data) {
        return serializationService.toObject(data);
    }

    public void handle(int type, Collection<CacheEventData> keys, int completionId) {
        try {
            if (CacheEventType.getByType(type) != CacheEventType.COMPLETED) {
                handleEvent(type, keys);
            }
        } finally {
            ((CacheSyncListenerCompleter) source).countDownCompletionLatch(completionId);
        }
    }

    @Override
    public void onRegister(CacheService cacheService, String serviceName,
                           String topic, EventRegistration registration) {
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(topic);
        cacheContext.increaseCacheEntryListenerCount();
    }

    @Override
    public void onDeregister(CacheService cacheService, String serviceName,
                             String topic, EventRegistration registration) {
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(topic);
        cacheContext.decreaseCacheEntryListenerCount();
    }

    @Override
    public boolean eval(Object event) {
        return true;
    }

    @Override
    public Object getListener() {
        return this;
    }

}
