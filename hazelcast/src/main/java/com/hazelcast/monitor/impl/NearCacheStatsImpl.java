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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

public class NearCacheStatsImpl implements NearCacheStats {

    private static final double PERCENTAGE = 100.0;

    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> OWNED_ENTRY_COUNT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(NearCacheStatsImpl.class, "ownedEntryCount");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> OWNED_ENTRY_MEMORY_COST_UPDATER =
            AtomicLongFieldUpdater.newUpdater(NearCacheStatsImpl.class, "ownedEntryMemoryCost");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> HITS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(NearCacheStatsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> MISSES_UPDATER =
            AtomicLongFieldUpdater.newUpdater(NearCacheStatsImpl.class, "misses");

    private volatile long creationTime;
    private volatile long ownedEntryCount;
    private volatile long ownedEntryMemoryCost;
    private volatile long hits;
    private volatile long misses;

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
        OWNED_ENTRY_COUNT_UPDATER.set(this, ownedEntryCount);
    }

    public void incrementOwnedEntryCount() {
        OWNED_ENTRY_COUNT_UPDATER.incrementAndGet(this);
    }

    public void decrementOwnedEntryCount() {
        OWNED_ENTRY_COUNT_UPDATER.decrementAndGet(this);
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST_UPDATER.set(this, ownedEntryMemoryCost);
    }

    public void incrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST_UPDATER.addAndGet(this, ownedEntryMemoryCost);
    }

    public void decrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        OWNED_ENTRY_MEMORY_COST_UPDATER.addAndGet(this, -ownedEntryMemoryCost);
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void incrementHits() {
        HITS_UPDATER.incrementAndGet(this);
    }

    public void setHits(long hits) {
        HITS_UPDATER.set(this, hits);
    }

    @Override
    public long getMisses() {
        return misses;
    }

    public void setMisses(long misses) {
        MISSES_UPDATER.set(this, misses);
    }

    public void incrementMisses() {
        MISSES_UPDATER.incrementAndGet(this);
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
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        root.add("creationTime", creationTime);
        root.add("hits", hits);
        root.add("misses", misses);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        hits = getLong(json, "hits", -1L);
        misses = getLong(json, "misses", -1L);
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
                + '}';
    }
}
