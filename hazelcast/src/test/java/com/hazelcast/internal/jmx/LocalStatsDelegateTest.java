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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.internal.jmx.suppliers.LocalMapStatsSupplier;
import com.hazelcast.internal.jmx.suppliers.StatsSupplier;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalStatsDelegateTest extends HazelcastTestSupport {

    private AtomicBoolean done;
    private HazelcastInstance hz;

    @Before
    public void setUp() {
        hz = createHazelcastInstance();
        done = new AtomicBoolean(false);
    }

    @Test
    public void testMapStats_Created_OnlyOnce_InGivenInternal() throws InterruptedException {
        final IMap<Object, Object> trial = hz.getMap("trial");

        final StatsSupplier<LocalMapStats> statsSupplier = new LocalMapStatsSupplier(trial);
        final LocalStatsDelegate localStatsDelegate = new LocalStatsDelegate<LocalMapStats>(statsSupplier, 60);

        Thread mapStatsThread1 = new Thread(new MapStatsThread(localStatsDelegate, done, 100));
        Thread mapStatsThread2 = new Thread(new MapStatsThread(localStatsDelegate, done, 90));

        mapStatsThread1.start();
        mapStatsThread2.start();

        Thread mapPutThread = new Thread(new MapPutThread(trial, done));
        mapPutThread.start();

        sleepSeconds(30);

        done.set(true);

        mapStatsThread1.join();
        mapStatsThread2.join();
        mapPutThread.join();
    }

    /**
     * In this test, map entry count is incrementing steadily. Thus, we expect OwnedEntryCount
     * to increase as well. Since the interval value for generating new map stats is 5 seconds,
     * we assert for greater or equal.
     */
    @Test
    @Category(NightlyTest.class)
    public void stressTest() throws InterruptedException {
        final IMap<Object, Object> trial = hz.getMap("trial");

        final StatsSupplier<LocalMapStats> statsSupplier = new LocalMapStatsSupplier(trial);
        final LocalStatsDelegate localStatsDelegate = new LocalStatsDelegate<LocalMapStats>(statsSupplier, 5);

        Thread mapStatsThread1 = new Thread(new MapStatsThread(localStatsDelegate, done, true, 100));
        Thread mapStatsThread2 = new Thread(new MapStatsThread(localStatsDelegate, done, true, 80));

        mapStatsThread1.start();
        mapStatsThread2.start();

        Thread mapPutThread = new Thread(new MapPutThread(trial, done));
        mapPutThread.start();

        sleepSeconds(60);

        done.set(true);

        mapStatsThread1.join();
        mapStatsThread2.join();
        mapPutThread.join();
    }

    private void assertGreaterThanOrEqualTo(long leftHand, long rightHand) {
        boolean correct = false;

        if (leftHand >= rightHand || leftHand == rightHand) {
            correct = true;
        }
        assertTrue(leftHand + " is less than " + rightHand, correct);
    }

    private class MapPutThread implements Runnable {
        private IMap map;
        private AtomicBoolean done;

        MapPutThread(IMap map, AtomicBoolean done) {
            this.map = map;
            this.done = done;
        }

        @Override
        public void run() {
            while (!done.get()) {
                String randomString = UuidUtil.newUnsecureUuidString();
                map.put(randomString, randomString);
                sleepMillis(10);
            }
        }
    }

    private class MapStatsThread implements Runnable {

        private LocalStatsDelegate localStatsDelegate;
        private AtomicBoolean done;

        private boolean stress;

        private int sleepMs;

        MapStatsThread(LocalStatsDelegate localMapStatsDelegate, AtomicBoolean done, int sleepMs) {
            this.localStatsDelegate = localMapStatsDelegate;
            this.done = done;
            this.sleepMs = sleepMs;
        }

        MapStatsThread(LocalStatsDelegate localMapStatsDelegate, AtomicBoolean done, boolean stress, int sleepMs) {
            this.localStatsDelegate = localMapStatsDelegate;
            this.done = done;
            this.stress = stress;
            this.sleepMs = sleepMs;
        }

        @Override
        public void run() {
            LocalMapStats localMapStats = (LocalMapStats) localStatsDelegate.getLocalStats();
            long previous = localMapStats.getOwnedEntryCount();
            long current = previous;

            while (!done.get()) {
                if (stress) {
                    assertGreaterThanOrEqualTo(current, previous);
                } else {
                    assertEquals(current, previous);
                }
                previous = current;
                localMapStats = (LocalMapStats) localStatsDelegate.getLocalStats();
                current = localMapStats.getOwnedEntryCount();
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
