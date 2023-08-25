/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.nearcache.NearCacheStats;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.LongAccumulator;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_EVICTIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_EXPIRATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_HITS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_INVALIDATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_INVALIDATION_REQUESTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_LAST_PERSISTENCE_DURATION;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_LAST_PERSISTENCE_KEY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_LAST_PERSISTENCE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_LAST_PERSISTENCE_WRITTEN_BYTES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_MISSES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_OWNED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_OWNED_ENTRY_MEMORY_COST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_METRIC_PERSISTENCE_COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.lang.String.format;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

@SuppressWarnings("checkstyle:methodcount")
public class NearCacheStatsImpl implements NearCacheStats {

    private static final double PERCENTAGE = 100.0;

    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> OWNED_ENTRY_COUNT =
            newUpdater(NearCacheStatsImpl.class, "ownedEntryCount");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> OWNED_ENTRY_MEMORY_COST =
            newUpdater(NearCacheStatsImpl.class, "ownedEntryMemoryCost");
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

    @Probe(name = NEARCACHE_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;
    @Probe(name = NEARCACHE_METRIC_OWNED_ENTRY_COUNT)
    private volatile long ownedEntryCount;
    @Probe(name = NEARCACHE_METRIC_OWNED_ENTRY_MEMORY_COST, unit = BYTES)
    private volatile long ownedEntryMemoryCost;
    @Probe(name = NEARCACHE_METRIC_HITS)
    private final LongAccumulator hits = new LongAccumulator(Long::sum, 0);
    @Probe(name = NEARCACHE_METRIC_MISSES)
    private final LongAccumulator misses = new LongAccumulator(Long::sum, 0);
    @Probe(name = NEARCACHE_METRIC_EVICTIONS)
    private volatile long evictions;
    @Probe(name = NEARCACHE_METRIC_EXPIRATIONS)
    private volatile long expirations;

    @Probe(name = NEARCACHE_METRIC_INVALIDATIONS)
    private volatile long invalidations;
    @Probe(name = NEARCACHE_METRIC_INVALIDATION_REQUESTS)
    private volatile long invalidationRequests;

    @Probe(name = NEARCACHE_METRIC_PERSISTENCE_COUNT)
    private volatile long persistenceCount;
    @Probe(name = NEARCACHE_METRIC_LAST_PERSISTENCE_TIME, unit = MS)
    private volatile long lastPersistenceTime;
    @Probe(name = NEARCACHE_METRIC_LAST_PERSISTENCE_DURATION, unit = MS)
    private volatile long lastPersistenceDuration;
    @Probe(name = NEARCACHE_METRIC_LAST_PERSISTENCE_WRITTEN_BYTES, unit = BYTES)
    private volatile long lastPersistenceWrittenBytes;
    @Probe(name = NEARCACHE_METRIC_LAST_PERSISTENCE_KEY_COUNT)
    private volatile long lastPersistenceKeyCount;
    private volatile String lastPersistenceFailure = "";

    public NearCacheStatsImpl() {
        this.creationTime = getNowInMillis();
    }

    public NearCacheStatsImpl(NearCacheStats stats) {
        creationTime = stats.getCreationTime();
        ownedEntryCount = stats.getOwnedEntryCount();
        ownedEntryMemoryCost = stats.getOwnedEntryMemoryCost();
        setHits(stats.getHits());
        setMisses(stats.getMisses());
        evictions = stats.getEvictions();
        expirations = stats.getExpirations();
        invalidations = stats.getInvalidations();
        invalidationRequests = stats.getInvalidationRequests();

        persistenceCount = stats.getPersistenceCount();
        lastPersistenceTime = stats.getLastPersistenceTime();
        lastPersistenceDuration = stats.getLastPersistenceDuration();
        lastPersistenceWrittenBytes = stats.getLastPersistenceWrittenBytes();
        lastPersistenceKeyCount = stats.getLastPersistenceKeyCount();
        lastPersistenceFailure = stats.getLastPersistenceFailure();
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
        return hits.longValue();
    }

    // just for testing
    void setHits(long hits) {
        this.hits.reset();
        this.hits.accumulate(hits);
    }

    public void incrementHits() {
        hits.accumulate(1);
    }

    @Override
    public long getMisses() {
        return misses.longValue();
    }

    // just for testing
    void setMisses(long misses) {
        this.misses.reset();
        this.misses.accumulate(misses);
    }

    public void incrementMisses() {
        misses.accumulate(1);
    }

    @Override
    public double getRatio() {
        if (misses.get() == 0) {
            if (hits.get() == 0) {
                return Double.NaN;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        } else {
            return hits.doubleValue() / misses.doubleValue() * PERCENTAGE;
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

    @Override
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
                + ", invalidationRequests=" + invalidationRequests
                + ", lastPersistenceTime=" + lastPersistenceTime
                + ", persistenceCount=" + persistenceCount
                + ", lastPersistenceDuration=" + lastPersistenceDuration
                + ", lastPersistenceWrittenBytes=" + lastPersistenceWrittenBytes
                + ", lastPersistenceKeyCount=" + lastPersistenceKeyCount
                + ", lastPersistenceFailure='" + lastPersistenceFailure + "'"
                + '}';
    }
}
