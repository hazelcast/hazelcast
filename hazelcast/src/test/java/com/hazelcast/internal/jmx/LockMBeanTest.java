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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockMBeanTest extends HazelcastTestSupport {

    private static final String TYPE_NAME = "ILock";

    private TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder = new MBeanDataHolder(hazelcastInstanceFactory);

    private ILock lock;
    private String objectName;

    @Before
    public void setUp() {
        lock = holder.getHz().getLock("lock");
        objectName = lock.getName();

        holder.assertMBeanExistEventually(TYPE_NAME, objectName);
    }

    @After
    public void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testLockCountIsTwo_whenLockedTwice() throws Exception {
        lock.lock(10000, TimeUnit.MILLISECONDS);
        lock.lock(10000, TimeUnit.MILLISECONDS);

        // check number of times locked (locked twice now)
        int lockCount = getIntegerAttribute("lockCount");

        assertEquals(2, lockCount);
    }

    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_remainingLeaseTimeCannotBeGreaterThanOriginal() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        long remainingLeaseTime = getLongAttribute("remainingLeaseTime");
        assertFalse(remainingLeaseTime > 1000);
    }

    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_mustBeLockedWhenHasRemainingLease() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        boolean isLocked = lock.isLockedByCurrentThread();
        long remainingLeaseTime = getLongAttribute("remainingLeaseTime");
        boolean hasRemainingLease = remainingLeaseTime > 0;
        assertTrue(isLocked || !hasRemainingLease);
    }


    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_eitherHasRemainingLeaseOrIsUnlocked() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        long remainingLeaseTime = getLongAttribute("remainingLeaseTime");
        boolean hasLeaseRemaining = remainingLeaseTime > 0;
        boolean isLocked = lock.isLockedByCurrentThread();
        assertTrue(!isLocked || hasLeaseRemaining);
    }

    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_mustHaveRemainingLeaseBeforeItExpires() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);
        long startTime = Clock.currentTimeMillis();

        long remainingLeaseTime = getLongAttribute("remainingLeaseTime");
        long timePassed = Clock.currentTimeMillis() - startTime;
        boolean hasLeaseRemaining = remainingLeaseTime > 0;

        assertTrue(hasLeaseRemaining || timePassed >= 1000);
    }

    @Test
    public void testIsNotLocked_whenMBeanForceUnlocked() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        holder.invokeMBeanOperation(TYPE_NAME, objectName, "forceUnlock", null, null);

        assertFalse("Lock is still locked, although forceUnlock has been called", lock.isLocked());
    }

    private Long getLongAttribute(String name) throws Exception {
        return (Long) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }

    private Integer getIntegerAttribute(String name) throws Exception {
        return (Integer) holder.getMBeanAttribute(TYPE_NAME, objectName, name);
    }
}
