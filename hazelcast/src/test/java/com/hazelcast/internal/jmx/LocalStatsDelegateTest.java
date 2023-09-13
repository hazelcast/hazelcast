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

package com.hazelcast.internal.jmx;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.jmx.suppliers.LocalMapStatsSupplier;
import com.hazelcast.internal.jmx.suppliers.StatsSupplier;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalStatsDelegateTest extends HazelcastTestSupport {

    private AtomicBoolean done;

    @Before
    public void setUp() {
        done = new AtomicBoolean(false);
    }

    @Test
    @Category(NightlyTest.class)
    public void testMapStats_Created_OnlyOnce_InGivenInternal() throws Exception {
        test(false, 60, 100, 90, 30);
    }

    /**
     * In this test, map entry count is incrementing steadily. Thus, we expect OwnedEntryCount to increase as well. Since the
     * interval value for generating new map stats is 5 seconds, we assert for greater or equal.
     */
    @Test
    @Category(NightlyTest.class)
    public void stressTest() throws Exception {
        test(true, 5, 100, 80, 60);
    }

    private void test(final boolean stress, final int intervalSec, final int mapStatsThread1SleepMs,
            final int mapStatsThread2SleepMs, final int sleepSeconds) throws Exception {
        final IMap<Object, Object> trial = createHazelcastInstance().getMap("trial");

        final StatsSupplier<LocalMapStats> statsSupplier = new LocalMapStatsSupplier(trial);
        final LocalStatsDelegate<LocalMapStats> localStatsDelegate = new LocalStatsDelegate<>(statsSupplier, intervalSec);

        final Collection<Future<Void>> mapStatsThreads = IntStream.of(mapStatsThread1SleepMs, mapStatsThread2SleepMs)
                .mapToObj(sleepMs -> new MapStatsThread(localStatsDelegate, stress, sleepMs)).map(HazelcastTestSupport::spawn)
                .collect(Collectors.toList());

        final Thread mapPutThread = new Thread(new MapPutThread(trial));
        mapPutThread.start();

        sleepSeconds(sleepSeconds);

        done.set(true);

        FutureUtil.waitForever(mapStatsThreads);
        mapPutThread.join();
    }

    private class MapPutThread implements Runnable {
        private final IMap<Object, Object> map;

        MapPutThread(final IMap<Object, Object> map) {
            this.map = map;
        }

        @Override
        public void run() {
            while (!done.get()) {
                final String randomString = UuidUtil.newUnsecureUuidString();
                map.put(randomString, randomString);
                sleepMillis(10);
            }
        }
    }

    private class MapStatsThread implements Callable<Void> {

        private final LocalStatsDelegate<LocalMapStats> localStatsDelegate;

        private final boolean stress;

        private final int sleepMs;

        MapStatsThread(final LocalStatsDelegate<LocalMapStats> localStatsDelegate, final boolean stress, final int sleepMs) {
            this.localStatsDelegate = localStatsDelegate;
            this.stress = stress;
            this.sleepMs = sleepMs;
        }

        @Override
        public Void call() {
            LocalMapStats localMapStats = localStatsDelegate.getLocalStats();
            long previous = localMapStats.getOwnedEntryCount();
            long current = previous;

            while (!done.get()) {
                if (stress) {
                    assertGreaterOrEquals("owned entry count", current, previous);
                } else {
                    assertEquals(current, previous);
                }
                previous = current;
                localMapStats = localStatsDelegate.getLocalStats();
                current = localMapStats.getOwnedEntryCount();
                sleepMillis(sleepMs);
            }

            return null;
        }
    }
}
