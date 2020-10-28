/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.test.ManageableClock.ManagedClock;
import com.hazelcast.test.ManageableClock.SyncedClock;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManageableClockTest {

    @Test
    public void testUnmanagedClockAdvances() {
        ManageableClock clock = new ManageableClock();

        long ts1 = clock.currentTimeMillis();
        sleepMillis(10);
        long ts2 = clock.currentTimeMillis();

        assertTrue(ts2 > ts1);
    }

    @Test
    public void testManagedClock() {
        ManageableClock clock = new ManageableClock();

        long notManagedTs = clock.currentTimeMillis();

        ManagedClock managedClock = clock.manage();
        long managedTs1 = managedClock.currentTimeMillis();
        assertTrue(managedTs1 >= notManagedTs);

        final int delta = -100;
        long managedTs2 = managedClock.advanceMillis(delta).currentTimeMillis();
        sleepMillis(10);
        assertEquals(managedTs1 + delta, managedTs2);
        assertEquals(managedTs2, clock.currentTimeMillis());

        ManageableClock unmanagedClock = managedClock.unmanage();
        assertSame(clock, unmanagedClock);
        assertTrue(clock.currentTimeMillis() >= notManagedTs);
        assertTrue(clock.currentTimeMillis() > managedTs2);
    }

    @Test
    public void testManageTwoClocks() {
        ManageableClock clock1 = new ManageableClock();
        ManageableClock clock2 = new ManageableClock();

        long shiftedClock1Ts = clock1.manage().advanceMillis(1000).currentTimeMillis();
        long shiftedClock2Ts = clock2.manage().advanceMillis(10000).currentTimeMillis();
        assertTrue(shiftedClock2Ts - shiftedClock1Ts > 5000);

        shiftedClock1Ts = clock1.currentTimeMillis();
        shiftedClock2Ts = clock2.currentTimeMillis();
        assertTrue(shiftedClock2Ts - shiftedClock1Ts > 5000);
    }

    @Test
    public void testSyncedClock() {
        ManageableClock masterClock = new ManageableClock();
        ManageableClock testedClock = new ManageableClock();

        SyncedClock syncedClock = testedClock.syncTo(masterClock);
        ManagedClock managedMasterClock = masterClock.manage().advanceMillis(-100);
        long managedTs = managedMasterClock.currentTimeMillis();
        sleepMillis(10);
        assertEquals(managedTs, managedMasterClock.currentTimeMillis());
        assertEquals(managedTs, testedClock.currentTimeMillis());
        assertEquals(managedTs, syncedClock.currentTimeMillis());

        ManageableClock unsyncedClock = syncedClock.unsync();
        assertSame(testedClock, unsyncedClock);
        assertTrue(testedClock.currentTimeMillis() > managedTs);
    }

    @Test
    public void testCallingUnmanageThrowsInSystemMode() {
        ManageableClock clock = new ManageableClock();

        assertThrows(AssertionError.class, clock::unmanage);
    }

    @Test
    public void testCallingUnmanageThrowsInSynchronizedMode() {
        ManageableClock clock = new ManageableClock();
        clock.syncTo(new ManageableClock());

        assertThrows(AssertionError.class, clock::unmanage);
    }

    @Test
    public void testShiftMillisThrowsInSystemMode() {
        ManageableClock clock = new ManageableClock();
        ManagedClock managedClock = clock.manage();
        clock.unmanage();

        assertThrows(AssertionError.class, () -> managedClock.advanceMillis(1));
    }


    @Test
    public void testCallingUnsyncThrowsInSystemMode() {
        ManageableClock clock = new ManageableClock();

        assertThrows(AssertionError.class, clock::unsync);
    }

    @Test
    public void testCallingUnsyncThrowsInManagedMode() {
        ManageableClock clock = new ManageableClock();
        clock.manage();

        assertThrows(AssertionError.class, clock::unsync);
    }

}
