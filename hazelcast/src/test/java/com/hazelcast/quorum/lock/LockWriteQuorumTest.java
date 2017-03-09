/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.lock;

import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LockWriteQuorumTest extends AbstractLockQuorumTest {

    @BeforeClass
    public static void initialize() throws Exception {
        initializeFiveMemberCluster(QuorumType.WRITE, 3);
        cluster.splitFiveMembersThreeAndTwo();
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testReadOnlyOkWhenQuorumSizeNotMet() {
        l4.getRemainingLeaseTime();
        l4.isLocked();
        l4.isLockedByCurrentThread();
        l5.getLockCount();
    }

    @Test(expected = QuorumException.class)
    public void testTryLockFailsWhenQuorumSizeNotMet() {
        l4.tryLock();
    }

    @Test(expected = QuorumException.class)
    public void testLockFailsWhenQuorumSizeNotMet() {
        l4.lock();
    }

    @Test(expected = QuorumException.class)
    public void testForceUnlockFailsWhenQuorumSizeNotMet() {
        l5.forceUnlock();
    }

    @Test(expected = QuorumException.class)
    public void testUnlockFailsWhenQuorumSizeNotMet() {
        l4.unlock();
    }

    @Test(expected = QuorumException.class)
    public void testAwaitFailsWhenQuorumSizeNotMet() throws Exception {
        l4.newCondition("condition").await();
    }

    @Test(expected = QuorumException.class)
    public void testSignalFailsWhenQuorumSizeNotMet() {
        l4.newCondition("condition").signal();
    }
}
