/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTime;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;

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

    @Nonnull
    protected ExpirySystem createExpirySystem(MapContainer mapContainer) {
        return new ExpirySystem(this, mapContainer, mapServiceContext);
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean backup) {
        expirySystem.evictExpiredEntries(getNow(), percentage, backup);
    }

    @Override
    public boolean isExpirable() {
        return expirySystem.isRecordStoreExpirable();
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

    protected void markRecordStoreExpirable(Data key, long ttl, long maxIdle, long now) {
        expirySystem.markRecordStoreExpirable(key, ttl, maxIdle, now);
    }

    @Override
    public boolean isTtlOrMaxIdleDefined(Record record) {
        return expirySystem.isTtlOrMaxIdleDefined(record);
    }

    @Override
    public boolean evictIfExpired(Data key, long now, boolean backup) {
        if (isLocked(key)) {
            return false;
        }
        if (!isExpired(key, now, backup)) {
            return false;
        }
        Object value = evict(key, backup);
        if (!backup) {
            doPostEvictionOperations(key, value);
        }
        return true;
    }

    @Override
    public boolean isExpired(Data dataKey, long now, boolean backup) {
        return expirySystem.isExpired(dataKey, now, backup);
    }

    // TODO optimize for HD access to read expiry metadata
    @Override
    public boolean expireOrAccess(Data key) {
        long now = Clock.currentTimeMillis();
        boolean expired = evictIfExpired(key, now, false);
        if (!expired) {
            Record record = storage.get(key);
            accessRecord(key, record, now);
        }
        return expired;
    }

    @Override
    public void doPostEvictionOperations(Data dataKey, Object value) {
        long now = getNow();
        boolean idleExpired = expirySystem.isIdleExpired(dataKey, now, false);
        boolean ttlExpired = expirySystem.isTTLExpired(dataKey, now, false);
        boolean expired = idleExpired || ttlExpired;

        if (eventService.hasEventRegistration(SERVICE_NAME, name)) {
            mapEventPublisher.publishEvent(thisAddress, name,
                    expired ? EXPIRED : EVICTED, dataKey, value, null);
        }

        if (!ttlExpired && idleExpired) {
            // only send expired key to backup if
            // it is expired according to idleness.
            expirySystem.accumulateOrSendExpiredKey(dataKey);
        }

        expirySystem.informEvicted(dataKey);
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
        // TODO set same time to record also
        setExpirationTime(record);
    }

    protected void mergeRecordExpiration(Data key, Record record, MapMergeTypes mergingEntry, long now) {
        mergeRecordExpiration(record, mergingEntry.getTtl(), mergingEntry.getMaxIdle(), mergingEntry.getCreationTime(),
                mergingEntry.getLastAccessTime(), mergingEntry.getLastUpdateTime());
        // TODO get ttl and maxIdle from this recordstores expiry system.
        markRecordStoreExpirable(key, record.getTtl(), record.getMaxIdle(), now);
    }

    private void mergeRecordExpiration(Record record, long ttlMillis, Long maxIdleMillis,
                                       long creationTime, long lastAccessTime, long lastUpdateTime) {
        record.setTtl(ttlMillis);
        // WAN events received from source cluster also carry null maxIdle
        // see com.hazelcast.map.impl.wan.WanMapEntryView.getMaxIdle
        if (maxIdleMillis != null) {
            record.setMaxIdle(maxIdleMillis);
        }
        record.setCreationTime(creationTime);
        record.setLastAccessTime(lastAccessTime);
        record.setLastUpdateTime(lastUpdateTime);

        setExpirationTime(record);
    }
}
