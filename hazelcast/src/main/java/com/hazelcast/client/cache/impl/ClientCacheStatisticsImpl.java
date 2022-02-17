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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.internal.monitor.impl.LocalReplicationStatsImpl;
import com.hazelcast.nearcache.NearCacheStats;

/**
 * {@link com.hazelcast.cache.CacheStatistics} implementation for client side {@link com.hazelcast.cache.ICache}.
 */
public class ClientCacheStatisticsImpl extends CacheStatisticsImpl {

    private NearCacheStats nearCacheStats;

    public ClientCacheStatisticsImpl(long creationTime) {
        super(creationTime);
    }

    @Override
    public long getOwnedEntryCount() {
        throw new UnsupportedOperationException("This statistic is not supported for client.");
    }

    @Override
    public long getCacheEvictions() {
        throw new UnsupportedOperationException("This statistic is not supported for client.");
    }

    @Override
    public long getCacheExpiries() {
        throw new UnsupportedOperationException("This statistic is not supported for client.");
    }

    @Override
    public NearCacheStats getNearCacheStatistics() {
        if (nearCacheStats == null) {
            throw new UnsupportedOperationException("Near Cache is not enabled.");

        }
        return nearCacheStats;
    }

    @Override
    public LocalReplicationStatsImpl getReplicationStats() {
        throw new UnsupportedOperationException("This statistic is not supported for client.");
    }

    @Override
    public String toString() {
        return "ClientCacheStatisticsImpl{"
                + "creationTime=" + creationTime
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", removals=" + removals
                + ", puts=" + puts
                + ", hits=" + hits
                + ", misses=" + misses
                + ", putTimeTakenNanos=" + putTimeTakenNanos
                + ", getCacheTimeTakenNanos=" + getCacheTimeTakenNanos
                + ", removeTimeTakenNanos=" + removeTimeTakenNanos
                + (nearCacheStats != null ? ", nearCacheStats=" + nearCacheStats : "")
                + '}';
    }

    public void setNearCacheStats(NearCacheStats nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }
}
