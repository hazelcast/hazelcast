/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class NearCacheStatsImpl implements NearCacheStats {

    private static final double PERCENTAGE = 100.0;

    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> OWNED_ENTRY_COUNT =
            newUpdater(NearCacheStatsImpl.class, "ownedEntryCount");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> OWNED_ENTRY_MEMORY_COST =
            newUpdater(NearCacheStatsImpl.class, "ownedEntryMemoryCost");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> HITS =
            newUpdater(NearCacheStatsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> MISSES =
            newUpdater(NearCacheStatsImpl.class, "misses");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> EVICTIONS =
            newUpdater(NearCacheStatsImpl.class, "evictions");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> EXPIRATIONS =
            newUpdater(NearCacheStatsImpl.class, "expirations");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> PERSISTENCE_COUNT =
            newUpdater(NearCacheStatsImpl.class, "persistenceCount");

    private volatile long creationTime;
    private volatile long ownedEntryCount;
    private volatile long ownedEntryMemoryCost;
    private volatile long hits;
    private volatile long misses;
    private volatile long evictions;
    private volatile long expirations;

    private volatile long persistenceCount;
    private volatile long lastPersistenceTime;
    private volatile long lastPersistenceDuration;
    private volatile long lastPersistenceWrittenBytes;
    private volatile long lastPersistenceKeyCount;

    public NearCacheStatsImpl() {
        this.creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        OWNED_ENTRY_COUNT.set(this, ownedEntryCount);
    }

    public void incrementOwnedEntryCount() {
        OWNED_ENTRY_COUNT.incrementAndGet(this);
    }

    public void decrementOwnedEntryCount() {
        OWNED_ENTRY_COUNT.decrementAndGet(this);
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST.set(this, ownedEntryMemoryCost);
    }

    public void incrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST.addAndGet(this, ownedEntryMemoryCost);
    }

    public void decrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST.addAndGet(this, -ownedEntryMemoryCost);
    }

    @Override
    public long getHits() {
        return hits;
    }

    // just for testing
    void setHits(long hits) {
        HITS.set(this, hits);
    }

    public void incrementHits() {
        HITS.incrementAndGet(this);
    }

    @Override
    public long getMisses() {
        return misses;
    }

    // just for testing
    void setMisses(long misses) {
        MISSES.set(this, misses);
    }

    public void incrementMisses() {
        MISSES.incrementAndGet(this);
    }

    @Override
    public double getRatio() {
        if (misses == 0) {
            if (hits == 0) {
                return Double.NaN;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        } else {
            return ((double) hits / misses) * PERCENTAGE;
        }
    }

    @Override
    public long getEvictions() {
        return evictions;
    }

    public void incrementEvictions() {
        EVICTIONS.incrementAndGet(this);
    }

    @Override
    public long getExpirations() {
        return expirations;
    }

    public void incrementExpirations() {
        EXPIRATIONS.incrementAndGet(this);
    }

    @Override
    public long getPersistenceCount() {
        return persistenceCount;
    }

    public void addPersistence(long duration, int writtenBytes, int keyCount) {
        PERSISTENCE_COUNT.incrementAndGet(this);
        lastPersistenceTime = Clock.currentTimeMillis();
        lastPersistenceDuration = duration;
        lastPersistenceWrittenBytes = writtenBytes;
        lastPersistenceKeyCount = keyCount;
    }

    @Override
    public long getLastPersistenceTime() {
        return lastPersistenceTime;
    }

    @Override
    public long getLastPersistenceDuration() {
        return lastPersistenceDuration;
    }

    @Override
    public long getLastPersistenceWrittenBytes() {
        return lastPersistenceWrittenBytes;
    }

    @Override
    public long getLastPersistenceKeyCount() {
        return lastPersistenceKeyCount;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        root.add("creationTime", creationTime);
        root.add("hits", hits);
        root.add("misses", misses);
        root.add("evictions", evictions);
        root.add("expirations", expirations);
        root.add("persistenceCount", persistenceCount);
        root.add("lastPersistenceTime", lastPersistenceTime);
        root.add("lastPersistenceDuration", lastPersistenceDuration);
        root.add("lastPersistenceWrittenBytes", lastPersistenceWrittenBytes);
        root.add("lastPersistenceKeyCount", lastPersistenceKeyCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        hits = getLong(json, "hits", -1L);
        misses = getLong(json, "misses", -1L);
        evictions = getLong(json, "evictions", -1L);
        expirations = getLong(json, "expirations", -1L);
        persistenceCount = getLong(json, "persistenceCount", -1L);
        lastPersistenceTime = getLong(json, "lastPersistenceTime", -1L);
        lastPersistenceDuration = getLong(json, "lastPersistenceDuration", -1L);
        lastPersistenceWrittenBytes = getLong(json, "lastPersistenceWrittenBytes", -1L);
        lastPersistenceKeyCount = getLong(json, "lastPersistenceKeyCount", -1L);
    }

    @Override
    public String toString() {
        return "NearCacheStatsImpl{"
                + "ownedEntryCount=" + ownedEntryCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", creationTime=" + creationTime
                + ", hits=" + hits
                + ", misses=" + misses
                + ", ratio=" + String.format("%.1f%%", getRatio())
                + ", evictions=" + evictions
                + ", expirations=" + expirations
                + ", lastPersistenceTime=" + lastPersistenceTime
                + ", persistenceCount=" + persistenceCount
                + ", lastPersistenceDuration=" + lastPersistenceDuration
                + ", lastPersistenceWrittenBytes=" + lastPersistenceWrittenBytes
                + ", lastPersistenceKeyCount=" + lastPersistenceKeyCount
                + '}';
    }
}
