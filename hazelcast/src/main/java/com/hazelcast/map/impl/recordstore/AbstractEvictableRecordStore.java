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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.EvictionChecker;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hazelcast.core.EntryEventType.EVICTED;
import static com.hazelcast.core.EntryEventType.EXPIRED;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateExpirationWithDelay;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.getIdlenessStartTime;
import static com.hazelcast.map.impl.ExpirationTimeSetter.getLifeStartTime;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTime;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;


/**
 * Contains eviction specific functionality.
 */
abstract class AbstractEvictableRecordStore extends AbstractRecordStore {

    private final long expiryDelayMillis;
    private final Evictor evictor;
    /**
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    private Iterator<Record> expirationIterator;
    /**
     * used in LRU eviction logic.
     */
    private long lruAccessSequenceNumber;
    /**
     * Last run time of cleanup operation.
     */
    private long lastEvictionTime;
    private volatile boolean hasEntryWithCustomTTL;

    protected AbstractEvictableRecordStore(MapContainer mapContainer, int partitionId) {
        super(mapContainer, partitionId);
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        expiryDelayMillis = groupProperties.getMillis(GroupProperty.MAP_EXPIRY_DELAY_SECONDS);
        evictor = mapContainer.getEvictor();
    }

    @Override
    public boolean isEvictionEnabled() {
        // this can be updated dynamically by UpdateMapConfigOperation.
        // that is why this check was put in a method instead of a class final field.
        EvictionPolicy evictionPolicy = getEvictionPolicy();
        return !EvictionPolicy.NONE.equals(evictionPolicy);
    }

    private EvictionPolicy getEvictionPolicy() {
        MapConfig mapConfig = mapContainer.getMapConfig();
        return mapConfig.getEvictionPolicy();
    }

    /**
     * Returns {@code true} if this record store has at least one candidate entry
     * for expiration (idle or tll) otherwise returns {@code false}.
     */
    private boolean isRecordStoreExpirable() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        return hasEntryWithCustomTTL || mapConfig.getMaxIdleSeconds() > 0
                || mapConfig.getTimeToLiveSeconds() > 0;
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean backup) {
        final long now = getNow();
        final int size = size();
        final int maxIterationCount = getMaxIterationCount(size, percentage);
        final int maxRetry = 3;
        int loop = 0;
        int evictedEntryCount = 0;
        while (true) {
            evictedEntryCount += evictExpiredEntriesInternal(maxIterationCount, now, backup);
            if (evictedEntryCount >= maxIterationCount) {
                break;
            }
            loop++;
            if (loop > maxRetry) {
                break;
            }
        }
    }

    @Override
    public boolean isExpirable() {
        return isRecordStoreExpirable();
    }

    /**
     * Intended to put an upper bound to iterations. Used in evictions.
     *
     * @param size       of iterate-able.
     * @param percentage percentage of size.
     * @return 100 If calculated iteration count is less than 100, otherwise returns calculated iteration count.
     */
    private int getMaxIterationCount(int size, int percentage) {
        final int defaultMaxIterationCount = 100;
        final float oneHundred = 100F;
        float maxIterationCount = size * (percentage / oneHundred);
        if (maxIterationCount <= defaultMaxIterationCount) {
            return defaultMaxIterationCount;
        }
        return Math.round(maxIterationCount);
    }

    private int evictExpiredEntriesInternal(int maxIterationCount, long now, boolean backup) {
        int evictedCount = 0;
        int checkedEntryCount = 0;
        initExpirationIterator();
        while (expirationIterator.hasNext()) {
            if (checkedEntryCount >= maxIterationCount) {
                break;
            }
            checkedEntryCount++;
            Record record = expirationIterator.next();
            record = getOrNullIfExpired(record, now, backup);
            if (record == null) {
                evictedCount++;
            }
        }
        return evictedCount;
    }

    private void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = storage.values().iterator();
        }
    }

    protected void resetAccessSequenceNumber() {
        lruAccessSequenceNumber = 0L;
    }

    @Override
    public void evictEntries(long now) {
        if (isEvictionEnabled()) {
            cleanUp(now);
        }
    }

    /**
     * Makes eviction clean-up logic.
     *
     * @param now now in millis.
     */
    private void cleanUp(long now) {
        if (size() == 0) {
            return;
        }
        if (shouldEvict(now)) {
            evict();
            lastEvictionTime = now;
        }
    }

    protected boolean shouldEvict(long now) {
        return isEvictionEnabled() && inEvictableTimeWindow(now) && checkEvictionPossible();
    }

    private void evict() {
        int removalSize = getRemovalSize();
        if (removalSize < 1) {
            return;
        }
        evictor.removeSize(removalSize, this);
    }

    private int getRemovalSize() {
        final int size = size();
        if (size < 1) {
            return 0;
        }
        int removalSize = evictor.findRemovalSize(this);
        if (removalSize < 1) {
            return 0;
        }
        return removalSize;
    }


    private Evictor getEvictor() {
        return mapContainer.getEvictor();
    }

    /**
     * Eviction waits at least {@link MapConfig#minEvictionCheckMillis} milliseconds to run.
     *
     * @return <code>true</code> if in that time window,
     * otherwise <code>false</code>
     */
    private boolean inEvictableTimeWindow(long now) {
        long minEvictionCheckMillis = getMinEvictionCheckMillis();
        return minEvictionCheckMillis == 0L
                || (now - lastEvictionTime) > minEvictionCheckMillis;
    }

    private long getMinEvictionCheckMillis() {
        MapConfig mapConfig = mapContainer.getMapConfig();
        return mapConfig.getMinEvictionCheckMillis();
    }

    private boolean checkEvictionPossible() {
        Evictor evictor = getEvictor();
        EvictionChecker evictionChecker = evictor.getEvictionChecker();
        return evictionChecker.checkEvictionPossible(this);
    }

    protected void markRecordStoreExpirable(long ttl) {
        if (ttl > 0L && ttl < Long.MAX_VALUE) {
            hasEntryWithCustomTTL = true;
        }
    }

    /**
     * Check if record is reachable according to ttl or idle times.
     * If not reachable return null.
     *
     * @param record {@link com.hazelcast.map.impl.record.Record}
     * @return null if evictable.
     */
    protected Record getOrNullIfExpired(Record record, long now, boolean backup) {
        if (!isRecordStoreExpirable()) {
            return record;
        }
        if (record == null) {
            return null;
        }
        final Data key = record.getKey();
        if (isLocked(key)) {
            return record;
        }
        if (!isExpired(record, now, backup)) {
            return record;
        }
        final Object value = record.getValue();
        evict(key, backup);
        if (!backup) {
            doPostEvictionOperations(key, value, true);
        }
        return null;
    }

    public boolean isExpired(Record record, long now, boolean backup) {
        return record == null
                || isIdleExpired(record, now, backup) == null
                || isTTLExpired(record, now, backup) == null;
    }

    private Record isIdleExpired(Record record, long now, boolean backup) {
        if (record == null) {
            return null;
        }

        long maxIdleMillis = calculateMaxIdleMillis(mapContainer.getMapConfig());
        if (maxIdleMillis == Long.MAX_VALUE) {
            return record;
        }
        long idlenessStartTime = getIdlenessStartTime(record);
        long idleMillis = calculateExpirationWithDelay(maxIdleMillis, expiryDelayMillis, backup);
        long elapsedMillis = now - idlenessStartTime;
        return elapsedMillis >= idleMillis ? null : record;
    }

    private Record isTTLExpired(Record record, long now, boolean backup) {
        if (record == null) {
            return null;
        }
        long ttl = record.getTtl();
        // when ttl is zero or negative or Long.MAX_VALUE, it should remain eternally.
        if (ttl < 1L || ttl == Long.MAX_VALUE) {
            return record;
        }
        long ttlStartTime = getLifeStartTime(record);
        long ttlMillis = calculateExpirationWithDelay(ttl, expiryDelayMillis, backup);
        long elapsedMillis = now - ttlStartTime;
        return elapsedMillis >= ttlMillis ? null : record;
    }

    /**
     * - Sends eviction event.
     * - Invalidates near cache.
     *
     * @param key   the key to be processed.
     * @param value the value to be processed.
     */
    @Override
    public void doPostEvictionOperations(Data key, Object value, boolean isExpired) {
        MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        EventService eventService = nodeEngine.getEventService();

        if (!eventService.hasEventRegistration(SERVICE_NAME, name)) {
            return;
        }

        Address thisAddress = nodeEngine.getThisAddress();

        // Fire EVICTED event also in case of expiration because historically eviction-listener
        // listens all kind of eviction and expiration events and by firing EVICTED event we are preserving
        // this behavior.
        mapEventPublisher.publishEvent(thisAddress, name, EVICTED, key, value, null);

        if (isExpired) {
            // We will be in this if in two cases:
            // 1. In case of TTL or max-idle-seconds expiration.
            // 2. When evicting due to the size-based eviction, we are also firing an EXPIRED event
            //    because there is a possibility that evicted entry may be also an expired one. Trying to catch
            //    as much as possible expired entries.
            mapEventPublisher.publishEvent(thisAddress, name, EXPIRED, key, value, null);
        }
    }

    void increaseRecordEvictionCriteriaNumber(Record record, EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                ++lruAccessSequenceNumber;
                record.setEvictionCriteriaNumber(lruAccessSequenceNumber);
                break;
            case LFU:
                record.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber() + 1L);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate eviction policy [" + evictionPolicy + ']');
        }
    }

    @Override
    protected void accessRecord(Record record, long now) {
        super.accessRecord(record, now);
        increaseRecordEvictionCriteriaNumber(record, getEvictionPolicy());

        final long maxIdleMillis = calculateMaxIdleMillis(mapContainer.getMapConfig());
        setExpirationTime(record, maxIdleMillis);
    }

    protected void mergeRecordExpiration(Record record, EntryView mergingEntry) {
        final long ttlMillis = mergingEntry.getTtl();
        record.setTtl(ttlMillis);

        final long creationTime = mergingEntry.getCreationTime();
        record.setCreationTime(creationTime);

        final long lastAccessTime = mergingEntry.getLastAccessTime();
        record.setLastAccessTime(lastAccessTime);

        final long lastUpdateTime = mergingEntry.getLastUpdateTime();
        record.setLastUpdateTime(lastUpdateTime);

        final long maxIdleMillis = calculateMaxIdleMillis(mapContainer.getMapConfig());
        setExpirationTime(record, maxIdleMillis);

        markRecordStoreExpirable(record.getTtl());
    }

    /**
     * Read only iterator. Iterates by checking whether a record expired or not.
     */
    protected final class ReadOnlyRecordIterator implements Iterator<Record> {

        private final long now;
        private final boolean checkExpiration;
        private final boolean backup;
        private final Iterator<Record> iterator;
        private Record nextRecord;
        private Record lastReturned;

        protected ReadOnlyRecordIterator(Collection<Record> values, long now, boolean backup) {
            this(values, now, true, backup);
        }

        protected ReadOnlyRecordIterator(Collection<Record> values) {
            this(values, -1L, false, false);
        }

        private ReadOnlyRecordIterator(Collection<Record> values, long now, boolean checkExpiration, boolean backup) {
            this.iterator = values.iterator();
            this.now = now;
            this.checkExpiration = checkExpiration;
            this.backup = backup;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public Record next() {
            if (nextRecord == null) {
                throw new NoSuchElementException();
            }
            lastReturned = nextRecord;
            advance();
            return lastReturned;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() is not supported by this iterator");
        }

        private void advance() {
            final long now = this.now;
            final boolean checkExpiration = this.checkExpiration;
            final Iterator<Record> iterator = this.iterator;

            while (iterator.hasNext()) {
                nextRecord = iterator.next();
                if (nextRecord != null) {
                    if (!checkExpiration) {
                        return;
                    }

                    if (!isExpired(nextRecord, now, backup)) {
                        return;
                    }
                }
            }
            nextRecord = null;
        }
    }
}
