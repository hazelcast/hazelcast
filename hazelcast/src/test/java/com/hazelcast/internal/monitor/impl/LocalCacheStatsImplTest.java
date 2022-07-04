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
import com.hazelcast.partition.LocalReplicationStats;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalCacheStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalCacheStatsImpl localCacheStats = new LocalCacheStatsImpl();

        assertEquals(0, localCacheStats.getCreationTime());
        assertEquals(0, localCacheStats.getLastUpdateTime());
        assertEquals(0, localCacheStats.getLastAccessTime());
        assertEquals(0, localCacheStats.getOwnedEntryCount());
        assertEquals(0, localCacheStats.getCacheHits());
        assertEquals(0, localCacheStats.getCacheHitPercentage(), 0.0001);
        assertEquals(0, localCacheStats.getCacheMisses());
        assertEquals(0, localCacheStats.getCacheMissPercentage(), 0.0001);
        assertEquals(0, localCacheStats.getCachePuts());
        assertEquals(0, localCacheStats.getCacheGets());
        assertEquals(0, localCacheStats.getCacheRemovals());
        assertEquals(0, localCacheStats.getCacheEvictions());
        assertEquals(0, localCacheStats.getAverageGetTime(), 0.0001);
        assertEquals(0, localCacheStats.getAveragePutTime(), 0.0001);
        assertEquals(0, localCacheStats.getAverageRemoveTime(), 0.0001);
        assertEquals(0, localCacheStats.getCreationTime());
        assertNotNull(localCacheStats.toString());
    }

    @Test
    public void testCacheStatisticsConstructor() {
        CacheStatistics cacheStatistics = new CacheStatistics() {
            @Override
            public long getCreationTime() {
                return 1986;
            }

            @Override
            public long getLastUpdateTime() {
                return 2014;
            }

            @Override
            public long getLastAccessTime() {
                return 2015;
            }

            @Override
            public long getOwnedEntryCount() {
                return 1000;
            }

            @Override
            public long getCacheHits() {
                return 127;
            }

            @Override
            public float getCacheHitPercentage() {
                return 12.5f;
            }

            @Override
            public long getCacheMisses() {
                return 5;
            }

            @Override
            public float getCacheMissPercentage() {
                return 11.4f;
            }

            @Override
            public long getCacheGets() {
                return 6;
            }

            @Override
            public long getCachePuts() {
                return 7;
            }

            @Override
            public long getCacheRemovals() {
                return 8;
            }

            @Override
            public long getCacheEvictions() {
                return 9;
            }

            @Override
            public float getAverageGetTime() {
                return 23.42f;
            }

            @Override
            public float getAveragePutTime() {
                return 42.23f;
            }

            @Override
            public float getAverageRemoveTime() {
                return 127.45f;
            }

            @Override
            public NearCacheStats getNearCacheStatistics() {
                return null;
            }

            @Override
            public LocalReplicationStats getReplicationStats() {
                return null;
            }
        };

        LocalCacheStatsImpl localCacheStats = new LocalCacheStatsImpl(cacheStatistics);

        assertEquals(1986, localCacheStats.getCreationTime());
        assertEquals(2014, localCacheStats.getLastUpdateTime());
        assertEquals(2015, localCacheStats.getLastAccessTime());
        assertEquals(1000, localCacheStats.getOwnedEntryCount());
        assertEquals(127, localCacheStats.getCacheHits());
        assertEquals(12.5f, localCacheStats.getCacheHitPercentage(), 0.0001);
        assertEquals(5, localCacheStats.getCacheMisses());
        assertEquals(11.4f, localCacheStats.getCacheMissPercentage(), 0.0001);
        assertEquals(6, localCacheStats.getCacheGets());
        assertEquals(7, localCacheStats.getCachePuts());
        assertEquals(8, localCacheStats.getCacheRemovals());
        assertEquals(9, localCacheStats.getCacheEvictions());
        assertEquals(23.42f, localCacheStats.getAverageGetTime(), 0.0001);
        assertEquals(42.23f, localCacheStats.getAveragePutTime(), 0.0001);
        assertEquals(127.45f, localCacheStats.getAverageRemoveTime(), 0.0001);
        assertTrue(localCacheStats.getCreationTime() > 0);
        assertNotNull(localCacheStats.toString());
    }
}
