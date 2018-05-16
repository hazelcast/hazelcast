/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.monitor.NearCacheStats;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getString;
import static java.lang.String.format;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

@SuppressWarnings("checkstyle:methodcount")
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
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> INVALIDATIONS =
            newUpdater(NearCacheStatsImpl.class, "invalidations");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> INVALIDATION_REQUESTS =
            newUpdater(NearCacheStatsImpl.class, "invalidationRequests");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> PERSISTENCE_COUNT =
            newUpdater(NearCacheStatsImpl.class, "persistenceCount");

    @Probe
    private volatile long creationTime;
    @Probe
    private volatile long ownedEntryCount;
    @Probe
    private volatile long ownedEntryMemoryCost;
    @Probe
    private volatile long hits;
    @Probe
    private volatile long misses;
    @Probe
    private volatile long evictions;
    @Probe
    private volatile long expirations;

    @Probe
    private volatile long invalidations;
    @Probe
    private volatile long invalidationRequests;

    @Probe
    private volatile long persistenceCount;
    @Probe
    private volatile long lastPersistenceTime;
    @Probe
    private volatile long lastPersistenceDuration;
    @Probe
    private volatile long lastPersistenceWrittenBytes;
    @Probe
    private volatile long lastPersistenceKeyCount;
    private volatile String lastPersistenceFailure = "";

    public NearCacheStatsImpl() {
        this.creationTime = getNowInMillis();
    }

    public NearCacheStatsImpl(NearCacheStats nearCacheStats) {
        NearCacheStatsImpl stats = (NearCacheStatsImpl) nearCacheStats;
        creationTime = stats.creationTime;
        ownedEntryCount = stats.ownedEntryCount;
        ownedEntryMemoryCost = stats.ownedEntryMemoryCost;
        hits = stats.hits;
        misses = stats.misses;
        evictions = stats.evictions;
        expirations = stats.expirations;
        invalidations = stats.invalidations;
        invalidationRequests = stats.invalidationRequests;

        persistenceCount = stats.persistenceCount;
        lastPersistenceTime = stats.lastPersistenceTime;
        lastPersistenceDuration = stats.lastPersistenceDuration;
        lastPersistenceWrittenBytes = stats.lastPersistenceWrittenBytes;
        lastPersistenceKeyCount = stats.lastPersistenceKeyCount;
        lastPersistenceFailure = stats.lastPersistenceFailure;
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
    public long getInvalidations() {
        return invalidations;
    }

    public void incrementInvalidations() {
        INVALIDATIONS.incrementAndGet(this);
    }

    public void incrementInvalidations(long delta) {
        INVALIDATIONS.addAndGet(this, delta);
    }

    public long getInvalidationRequests() {
        return invalidationRequests;
    }

    public void incrementInvalidationRequests() {
        INVALIDATION_REQUESTS.incrementAndGet(this);
    }

    public void resetInvalidationEvents() {
        INVALIDATION_REQUESTS.set(this, 0);
    }

    @Override
    public long getPersistenceCount() {
        return persistenceCount;
    }

    public void addPersistence(long duration, int writtenBytes, int keyCount) {
        PERSISTENCE_COUNT.incrementAndGet(this);
        lastPersistenceTime = getNowInMillis();
        lastPersistenceDuration = duration;
        lastPersistenceWrittenBytes = writtenBytes;
        lastPersistenceKeyCount = keyCount;
        lastPersistenceFailure = "";
    }

    public void addPersistenceFailure(Throwable t) {
        PERSISTENCE_COUNT.incrementAndGet(this);
        lastPersistenceTime = getNowInMillis();
        lastPersistenceDuration = 0;
        lastPersistenceWrittenBytes = 0;
        lastPersistenceKeyCount = 0;
        lastPersistenceFailure = t.getClass().getSimpleName() + ": " + t.getMessage();
    }

    private static long getNowInMillis() {
        return System.currentTimeMillis();
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
    public String getLastPersistenceFailure() {
        return lastPersistenceFailure;
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
        root.add("invalidations", invalidations);
        root.add("invalidationEvents", invalidationRequests);
        root.add("persistenceCount", persistenceCount);
        root.add("lastPersistenceTime", lastPersistenceTime);
        root.add("lastPersistenceDuration", lastPersistenceDuration);
        root.add("lastPersistenceWrittenBytes", lastPersistenceWrittenBytes);
        root.add("lastPersistenceKeyCount", lastPersistenceKeyCount);
        root.add("lastPersistenceFailure", lastPersistenceFailure);
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
        invalidations = getLong(json, "invalidations", -1L);
        invalidationRequests = getLong(json, "invalidationEvents", -1L);
        persistenceCount = getLong(json, "persistenceCount", -1L);
        lastPersistenceTime = getLong(json, "lastPersistenceTime", -1L);
        lastPersistenceDuration = getLong(json, "lastPersistenceDuration", -1L);
        lastPersistenceWrittenBytes = getLong(json, "lastPersistenceWrittenBytes", -1L);
        lastPersistenceKeyCount = getLong(json, "lastPersistenceKeyCount", -1L);
        lastPersistenceFailure = getString(json, "lastPersistenceFailure", "");
    }

    @Override
    public String toString() {
        return "NearCacheStatsImpl{"
                + "ownedEntryCount=" + ownedEntryCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", creationTime=" + creationTime
                + ", hits=" + hits
                + ", misses=" + misses
                + ", ratio=" + format("%.1f%%", getRatio())
                + ", evictions=" + evictions
                + ", expirations=" + expirations
                + ", invalidations=" + invalidations
                + ", lastPersistenceTime=" + lastPersistenceTime
                + ", persistenceCount=" + persistenceCount
                + ", lastPersistenceDuration=" + lastPersistenceDuration
                + ", lastPersistenceWrittenBytes=" + lastPersistenceWrittenBytes
                + ", lastPersistenceKeyCount=" + lastPersistenceKeyCount
                + ", lastPersistenceFailure='" + lastPersistenceFailure + "'"
                + '}';
    }
}
