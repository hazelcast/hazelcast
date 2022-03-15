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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryReason;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystemImpl;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.map.impl.recordstore.expiry.ExpiryReason.MAX_IDLE_SECONDS;
import static com.hazelcast.map.impl.recordstore.expiry.ExpiryReason.NOT_EXPIRED;

/**
 * Contains eviction specific functionality.
 */
public abstract class AbstractEvictableRecordStore extends AbstractRecordStore {
    protected final Address thisAddress;
    protected final EventService eventService;
    protected final MapEventPublisher mapEventPublisher;
    protected final ExpirySystem expirySystem;

    protected AbstractEvictableRecordStore(MapContainer mapContainer, int partitionId) {
        super(mapContainer, partitionId);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        eventService = nodeEngine.getEventService();
        mapEventPublisher = mapServiceContext.getMapEventPublisher();
        thisAddress = nodeEngine.getThisAddress();
        expirySystem = createExpirySystem(mapContainer);
    }

    @Override
    public ExpirySystem getExpirySystem() {
        return expirySystem;
    }

    @Nonnull
    protected ExpirySystem createExpirySystem(MapContainer mapContainer) {
        return new ExpirySystemImpl(this, mapContainer, mapServiceContext);
    }

    @Override
    public void evictExpiredEntries(int percentage, long now, boolean backup) {
        expirySystem.evictExpiredEntries(percentage, now, backup);
    }

    @Override
    public boolean isExpirable() {
        return !expirySystem.isEmpty();
    }

    @Override
    public void evictEntries(Data excludedKey) {
        if (shouldEvict()) {
            mapContainer.getEvictor().evict(this, excludedKey);
        }
    }

    @Override
    public void sampleAndForceRemoveEntries(int entryCountToRemove) {
        Queue<Data> keysToRemove = new LinkedList<>();
        Iterable<EntryView> sample = storage.getRandomSamples(entryCountToRemove);
        for (EntryView entryView : sample) {
            Data dataKey = storage.extractDataKeyFromLazy(entryView);
            keysToRemove.add(dataKey);
        }

        Data dataKey;
        while ((dataKey = keysToRemove.poll()) != null) {
            evict(dataKey, true);
        }
    }

    @Override
    public boolean shouldEvict() {
        Evictor evictor = mapContainer.getEvictor();
        return evictor != NULL_EVICTOR && evictor.checkEvictable(this);
    }

    @Override
    public boolean evictIfExpired(Data key, long now, boolean backup) {
        ExpiryReason expiryReason = hasExpired(key, now, backup);
        if (expiryReason == NOT_EXPIRED) {
            return false;
        }
        evictExpiredEntryAndPublishExpiryEvent(key, expiryReason, backup);
        return true;
    }

    @Override
    public void evictExpiredEntryAndPublishExpiryEvent(Data key,
                                                       ExpiryReason expiryReason,
                                                       boolean backup) {
        Object value = evict(key, backup);
        if (value != null && !backup) {
            doPostEvictionOperations(key, value, expiryReason);
        }
    }

    @Override
    public ExpiryReason hasExpired(Data key, long now, boolean backup) {
        if (expirySystem.isEmpty() || isLocked(key)) {
            return NOT_EXPIRED;
        }
        return expirySystem.hasExpired(key, now, backup);
    }

    @Override
    public boolean isExpired(Data key, long now, boolean backup) {
        return hasExpired(key, now, backup) != NOT_EXPIRED;
    }

    @Override
    public void doPostEvictionOperations(Data dataKey, Object value,
                                         ExpiryReason expiryReason) {
        if (eventService.hasEventRegistration(SERVICE_NAME, name)) {
            EntryEventType eventType = expiryReason != NOT_EXPIRED ? EXPIRED : EVICTED;
            mapEventPublisher.publishEvent(thisAddress, name,
                    eventType, dataKey, value, null);
        }

        if (expiryReason == MAX_IDLE_SECONDS) {
            // only send expired key to back-up if
            // it is expired according to idleness.
            expirySystem.accumulateOrSendExpiredKey(dataKey, value.hashCode());
        }
    }

    @Override
    public InvalidationQueue<ExpiredKey> getExpiredKeysQueue() {
        return expirySystem.getExpiredKeys();
    }

    @Override
    public void accessRecord(Data dataKey, Record record, long now) {
        record.onAccess(now);
        updateStatsOnGet(now);
        expirySystem.extendExpiryTime(dataKey, now);
    }

    protected void mergeRecordExpiration(Data key, Record record,
                                         MapMergeTypes mergingEntry, long now) {
        mergeRecordExpiration(record, mergingEntry.getCreationTime(),
                mergingEntry.getLastAccessTime(), mergingEntry.getLastUpdateTime());
        // WAN events received from source cluster also carry null maxIdle
        // see com.hazelcast.map.impl.wan.WanMapEntryView.getMaxIdle
        Long maxIdle = mergingEntry.getMaxIdle();
        if (maxIdle != null) {
            getExpirySystem().add(key, mergingEntry.getTtl(),
                    maxIdle, mergingEntry.getExpirationTime(), mergingEntry.getLastUpdateTime(), now);
        } else {
            ExpiryMetadata expiryMetadata = getExpirySystem().getExpiryMetadata(key);
            getExpirySystem().add(key, mergingEntry.getTtl(),
                    expiryMetadata.getMaxIdle(), mergingEntry.getExpirationTime(),
                    mergingEntry.getLastUpdateTime(), now);
        }
    }

    private void mergeRecordExpiration(Record record,
                                       long creationTime,
                                       long lastAccessTime, long lastUpdateTime) {
        record.setCreationTime(creationTime);
        record.setLastAccessTime(lastAccessTime);
        record.setLastUpdateTime(lastUpdateTime);
    }
}
