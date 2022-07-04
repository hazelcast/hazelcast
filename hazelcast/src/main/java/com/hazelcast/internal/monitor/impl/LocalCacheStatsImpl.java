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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.monitor.LocalCacheStats;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_AVERAGE_GET_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_AVERAGE_PUT_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_AVERAGE_REMOVAL_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_EVICTIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_GETS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_HITS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_HIT_PERCENTAGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_MISSES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_MISS_PERCENTAGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_PUTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CACHE_REMOVALS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_LAST_ACCESS_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_LAST_UPDATE_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CACHE_METRIC_OWNED_ENTRY_COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static com.hazelcast.internal.metrics.ProbeUnit.US;

/**
 * Default implementation of {@link com.hazelcast.internal.monitor.LocalCacheStats}
 * <p>
 * This class just provides serialization/deserialization methods to be used in
 * {@link com.hazelcast.internal.monitor.MemberState} implementation while sending/receiving statistics to/from
 * Management center.
 * <p>
 * There are no calculations are done in this class, all statistics gathered from
 * {@link com.hazelcast.cache.CacheStatistics}
 * <p>
 * No setter methods are provided, all class fields supposed to be populated by
 * a {@link com.hazelcast.cache.CacheStatistics}.
 *
 * @see com.hazelcast.cache.CacheStatistics
 */
public class LocalCacheStatsImpl implements LocalCacheStats {

    @Probe(name = CACHE_METRIC_CREATION_TIME, unit = MS)
    private long creationTime;
    @Probe(name = CACHE_METRIC_LAST_ACCESS_TIME, unit = MS)
    private long lastAccessTime;
    @Probe(name = CACHE_METRIC_LAST_UPDATE_TIME, unit = MS)
    private long lastUpdateTime;
    @Probe(name = CACHE_METRIC_OWNED_ENTRY_COUNT)
    private long ownedEntryCount;
    @Probe(name = CACHE_METRIC_CACHE_HITS)
    private long cacheHits;
    @Probe(name = CACHE_METRIC_CACHE_HIT_PERCENTAGE, unit = PERCENT)
    private float cacheHitPercentage;
    @Probe(name = CACHE_METRIC_CACHE_MISSES)
    private long cacheMisses;
    @Probe(name = CACHE_METRIC_CACHE_MISS_PERCENTAGE, unit = PERCENT)
    private float cacheMissPercentage;
    @Probe(name = CACHE_METRIC_CACHE_GETS)
    private long cacheGets;
    @Probe(name = CACHE_METRIC_CACHE_PUTS)
    private long cachePuts;
    @Probe(name = CACHE_METRIC_CACHE_REMOVALS)
    private long cacheRemovals;
    @Probe(name = CACHE_METRIC_CACHE_EVICTIONS)
    private long cacheEvictions;
    @Probe(name = CACHE_METRIC_AVERAGE_GET_TIME, unit = US)
    private float averageGetTime;
    @Probe(name = CACHE_METRIC_AVERAGE_PUT_TIME, unit = US)
    private float averagePutTime;
    @Probe(name = CACHE_METRIC_AVERAGE_REMOVAL_TIME, unit = US)
    private float averageRemoveTime;

    public LocalCacheStatsImpl() {
    }

    public LocalCacheStatsImpl(CacheStatistics cacheStatistics) {
        creationTime = cacheStatistics.getCreationTime();
        lastAccessTime = cacheStatistics.getLastAccessTime();
        lastUpdateTime = cacheStatistics.getLastUpdateTime();
        ownedEntryCount = cacheStatistics.getOwnedEntryCount();
        cacheHits = cacheStatistics.getCacheHits();
        cacheHitPercentage = cacheStatistics.getCacheHitPercentage();
        cacheMisses = cacheStatistics.getCacheMisses();
        cacheMissPercentage = cacheStatistics.getCacheMissPercentage();
        cacheGets = cacheStatistics.getCacheGets();
        cachePuts = cacheStatistics.getCachePuts();
        cacheRemovals = cacheStatistics.getCacheRemovals();
        cacheEvictions = cacheStatistics.getCacheEvictions();
        averageGetTime = cacheStatistics.getAverageGetTime();
        averagePutTime = cacheStatistics.getAveragePutTime();
        averageRemoveTime = cacheStatistics.getAverageRemoveTime();
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    @Override
    public long getCacheHits() {
        return cacheHits;
    }

    @Override
    public float getCacheHitPercentage() {
        return cacheHitPercentage;
    }

    @Override
    public long getCacheMisses() {
        return cacheMisses;
    }

    @Override
    public float getCacheMissPercentage() {
        return cacheMissPercentage;
    }

    @Override
    public long getCacheGets() {
        return cacheGets;
    }

    @Override
    public long getCachePuts() {
        return cachePuts;
    }

    @Override
    public long getCacheRemovals() {
        return cacheRemovals;
    }

    @Override
    public long getCacheEvictions() {
        return cacheEvictions;
    }

    @Override
    public float getAverageGetTime() {
        return averageGetTime;
    }

    @Override
    public float getAveragePutTime() {
        return averagePutTime;
    }

    @Override
    public float getAverageRemoveTime() {
        return averageRemoveTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public String toString() {
        return "LocalCacheStatsImpl{"
                + "creationTime=" + creationTime
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", ownedEntryCount=" + ownedEntryCount
                + ", cacheHits=" + cacheHits
                + ", cacheHitPercentage=" + cacheHitPercentage
                + ", cacheMisses=" + cacheMisses
                + ", cacheMissPercentage=" + cacheMissPercentage
                + ", cacheGets=" + cacheGets
                + ", cachePuts=" + cachePuts
                + ", cacheRemovals=" + cacheRemovals
                + ", cacheEvictions=" + cacheEvictions
                + ", averageGetTime=" + averageGetTime
                + ", averagePutTime=" + averagePutTime
                + ", averageRemoveTime=" + averageRemoveTime
                + '}';
    }
}
