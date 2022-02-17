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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NearCacheStatsImplTest extends HazelcastTestSupport {

    private NearCacheStatsImpl nearCacheStats;

    @Before
    public void setUp() {
        nearCacheStats = new NearCacheStatsImpl();

        nearCacheStats.setOwnedEntryCount(501);
        nearCacheStats.incrementOwnedEntryCount();
        nearCacheStats.decrementOwnedEntryCount();
        nearCacheStats.decrementOwnedEntryCount();

        nearCacheStats.setOwnedEntryMemoryCost(1024);
        nearCacheStats.incrementOwnedEntryMemoryCost(512);
        nearCacheStats.decrementOwnedEntryMemoryCost(256);

        nearCacheStats.setHits(600);
        nearCacheStats.incrementHits();
        nearCacheStats.incrementHits();

        nearCacheStats.setMisses(304);
        nearCacheStats.incrementMisses();

        nearCacheStats.incrementEvictions();
        nearCacheStats.incrementEvictions();
        nearCacheStats.incrementEvictions();
        nearCacheStats.incrementEvictions();

        nearCacheStats.incrementExpirations();
        nearCacheStats.incrementExpirations();
        nearCacheStats.incrementExpirations();

        nearCacheStats.incrementInvalidations(23);
        nearCacheStats.incrementInvalidations();

        nearCacheStats.incrementInvalidationRequests();
        nearCacheStats.incrementInvalidationRequests();

        nearCacheStats.addPersistence(200, 300, 400);
    }

    @Test
    public void testDefaultConstructor() {
        assertNearCacheStats(nearCacheStats, 1, 200, 300, 400, false);
    }

    @Test
    public void testCopyConstructor() {
        NearCacheStatsImpl copy = new NearCacheStatsImpl(nearCacheStats);

        assertNearCacheStats(copy, 1, 200, 300, 400, false);
    }

    @Test
    public void testGetRatio_NaN() {
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        assertEquals(Double.NaN, nearCacheStats.getRatio(), 0.0001);
    }

    @Test
    public void testGetRatio_POSITIVE_INFINITY() {
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        nearCacheStats.setHits(1);
        assertEquals(Double.POSITIVE_INFINITY, nearCacheStats.getRatio(), 0.0001);
    }

    @Test
    public void testGetRatio_100() {
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();
        nearCacheStats.setHits(1);
        nearCacheStats.setMisses(1);
        assertEquals(100d, nearCacheStats.getRatio(), 0.0001);
    }

    @Test
    public void testConcurrentModification() {
        int incThreads = 40;
        int decThreads = 10;
        int countPerThread = 500;

        CountDownLatch startLatch = new CountDownLatch(1);
        NearCacheStatsImpl nearCacheStats = new NearCacheStatsImpl();

        Thread[] threads = new Thread[incThreads + decThreads];
        for (int i = 0; i < incThreads; i++) {
            threads[i] = new StatsModifierThread(startLatch, nearCacheStats, true, countPerThread);
            threads[i].start();
        }
        for (int i = incThreads; i < incThreads + decThreads; i++) {
            threads[i] = new StatsModifierThread(startLatch, nearCacheStats, false, countPerThread);
            threads[i].start();
        }

        startLatch.countDown();
        assertJoinable(threads);

        System.out.println(nearCacheStats);

        int incCount = incThreads * countPerThread;
        int decCount = decThreads * countPerThread;
        int totalCount = incCount - decCount;
        assertEquals(totalCount, nearCacheStats.getOwnedEntryCount());
        assertEquals(totalCount * 23, nearCacheStats.getOwnedEntryMemoryCost());
        assertEquals(incCount, nearCacheStats.getHits());
        assertEquals(decCount, nearCacheStats.getMisses());
        assertEquals(incCount, nearCacheStats.getEvictions());
        assertEquals(incCount, nearCacheStats.getExpirations());
    }

    private static void assertNearCacheStats(NearCacheStatsImpl stats, long expectedPersistenceCount, long expectedDuration,
                                             long expectedWrittenBytes, long expectedKeyCount, boolean expectedFailure) {
        assertTrue(stats.getCreationTime() > 0);
        assertEquals(500, stats.getOwnedEntryCount());
        assertEquals(1280, stats.getOwnedEntryMemoryCost());
        assertEquals(602, stats.getHits());
        assertEquals(305, stats.getMisses());
        assertEquals(4, stats.getEvictions());
        assertEquals(3, stats.getExpirations());
        assertEquals(24, stats.getInvalidations());
        assertEquals(2, stats.getInvalidationRequests());
        assertEquals(expectedPersistenceCount, stats.getPersistenceCount());
        assertTrue(stats.getLastPersistenceTime() > 0);
        assertEquals(expectedDuration, stats.getLastPersistenceDuration());
        assertEquals(expectedWrittenBytes, stats.getLastPersistenceWrittenBytes());
        assertEquals(expectedKeyCount, stats.getLastPersistenceKeyCount());
        if (expectedFailure) {
            assertFalse(stats.getLastPersistenceFailure().isEmpty());
        } else {
            assertTrue(stats.getLastPersistenceFailure().isEmpty());
        }
        assertNotNull(stats.toString());
    }

    private static class StatsModifierThread extends Thread {

        private final CountDownLatch startLatch;
        private final NearCacheStatsImpl nearCacheStats;
        private final boolean increment;
        private final int count;

        private StatsModifierThread(CountDownLatch startLatch, NearCacheStatsImpl nearCacheStats, boolean increment, int count) {
            this.startLatch = startLatch;
            this.nearCacheStats = nearCacheStats;
            this.increment = increment;
            this.count = count;
        }

        @Override
        public void run() {
            assertOpenEventually(startLatch);
            for (int i = 0; i < count; i++) {
                if (increment) {
                    nearCacheStats.incrementOwnedEntryCount();
                    nearCacheStats.incrementOwnedEntryMemoryCost(23);
                    nearCacheStats.incrementHits();
                    nearCacheStats.incrementEvictions();
                    nearCacheStats.incrementExpirations();
                } else {
                    nearCacheStats.decrementOwnedEntryCount();
                    nearCacheStats.decrementOwnedEntryMemoryCost(23);
                    nearCacheStats.incrementMisses();
                }
            }
        }
    }
}
