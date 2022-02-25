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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheEventType;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchInvalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.internal.nearcache.impl.invalidation.NonStopInvalidator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.TRUE_FILTER;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;

/**
 * Sends cache invalidation events in batch or single as configured.
 */
public class CacheEventHandler {

    private final NodeEngine nodeEngine;
    private final Invalidator invalidator;

    CacheEventHandler(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.invalidator = createInvalidator();
    }

    private Invalidator createInvalidator() {
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        int batchSize = hazelcastProperties.getInteger(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE);
        int batchFrequencySeconds = hazelcastProperties.getInteger(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
        boolean batchingEnabled = hazelcastProperties.getBoolean(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;

        if (batchingEnabled) {
            return new BatchInvalidator(SERVICE_NAME, batchSize, batchFrequencySeconds, TRUE_FILTER, nodeEngine);
        } else {
            return new NonStopInvalidator(SERVICE_NAME, TRUE_FILTER, nodeEngine);
        }
    }

    public MetaDataGenerator getMetaDataGenerator() {
        return invalidator.getMetaDataGenerator();
    }

    void publishEvent(CacheEventContext cacheEventContext) {
        final EventService eventService = nodeEngine.getEventService();
        final String cacheName = cacheEventContext.getCacheName();
        final Collection<EventRegistration> candidates =
                eventService.getRegistrations(SERVICE_NAME, cacheName);

        if (candidates.isEmpty()) {
            return;
        }
        final Object eventData;
        final CacheEventType eventType = cacheEventContext.getEventType();
        switch (eventType) {
            case CREATED:
            case UPDATED:
            case REMOVED:
            case EXPIRED:
                final CacheEventData cacheEventData =
                        new CacheEventDataImpl(cacheName, eventType, cacheEventContext.getDataKey(),
                                cacheEventContext.getDataValue(), cacheEventContext.getDataOldValue(),
                                cacheEventContext.isOldValueAvailable());
                CacheEventSet eventSet = new CacheEventSet(eventType, cacheEventContext.getCompletionId());
                eventSet.addEventData(cacheEventData);
                eventData = eventSet;
                break;
            case EVICTED:
            case INVALIDATED:
                eventData = new CacheEventDataImpl(cacheName, eventType, cacheEventContext.getDataKey(),
                        null, null, false);
                break;
            case COMPLETED:
                CacheEventData completedEventData =
                        new CacheEventDataImpl(cacheName, eventType, cacheEventContext.getDataKey(),
                                cacheEventContext.getDataValue(), null, false);
                eventSet = new CacheEventSet(eventType, cacheEventContext.getCompletionId());
                eventSet.addEventData(completedEventData);
                eventData = eventSet;
                break;
            default:
                throw new IllegalArgumentException(
                        "Event Type not defined to create an eventData during publish: " + eventType.name());
        }
        eventService.publishEvent(SERVICE_NAME, candidates,
                eventData, cacheEventContext.getOrderKey());
    }

    void publishEvent(String cacheNameWithPrefix, CacheEventSet eventSet, int orderKey) {
        final EventService eventService = nodeEngine.getEventService();
        final Collection<EventRegistration> candidates =
                eventService.getRegistrations(SERVICE_NAME, cacheNameWithPrefix);
        if (candidates.isEmpty()) {
            return;
        }
        eventService.publishEvent(SERVICE_NAME, candidates, eventSet, orderKey);
    }

    void sendInvalidationEvent(String name, Data key, UUID sourceUuid) {
        if (key == null) {
            invalidator.invalidateAllKeys(name, sourceUuid);
        } else {
            invalidator.invalidateKey(key, name, sourceUuid);
        }
    }

    public void forceIncrementSequence(String name, int partitionId) {
        invalidator.forceIncrementSequence(name, partitionId);
    }

    public void destroy(String name, UUID sourceUuid) {
        invalidator.destroy(name, sourceUuid);
    }

    void shutdown() {
        invalidator.shutdown();
    }
}
