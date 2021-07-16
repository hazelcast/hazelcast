/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import static com.hazelcast.cp.internal.HazelcastRaftTestSupport.waitUntilCPDiscoveryCompleted;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import static org.junit.Assert.assertFalse;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Demonstrates locking issue when lock hangs in the locked state
 * when it should be in unlocked state
 *
 * This test is for issue https://github.com/hazelcast/hazelcast/issues/17260
 *
 * Sequence of events (simplest version):
 * - two nodes
 * - second (non-master) node is where all locking operations happen
 * - second node locks the lock
 * - master node is frozen out
 * - second node unlocks the lock
 * - master node unfrozen
 *
 * In this case, since the second node just locks and unlocks the same lock
 * when the master node is brought back on-line then the lock should remain unlocked
 * However, the lock becomes locked again about 90% of the time.
 *
 * Looks like master node directs the second node to be in it's remembered state,
 * which is wrong.
 *
 * @author lprimak
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeFencedLockSplitBrainTest extends SplitBrainTestSupport {
    private static final String LOCK_NAME = "myLock";

    @Override
    protected int[] brains() {
        // doesn't matter what these values are,
        // the test fails most of the time, but sporradically succeeds
        return new int[] {2, 2};
    }

    @Override
    protected Config config() {
        Config config = super.config();
        // if CP subsystem is enabled, the test becomes unstable with
        // spurious errors in different places, and also doesn't pass sometimes as well
//        config.getCPSubsystemConfig()
//                .setCPMemberCount(3)
//                .setGroupSize(3);
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        waitUntilCPDiscoveryCompleted(instances);

        // lock the non-master's lock
        FencedLock secondLock = getBrains().getSecondHalf()[0].getCPSubsystem().getLock(LOCK_NAME);
        secondLock.lock();
        sleepSeconds(3);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) throws Exception {
        // unlock the non-master's lock
        FencedLock secondLock = secondBrain[0].getCPSubsystem().getLock(LOCK_NAME);
        secondLock.unlock();
        sleepSeconds(3);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        sleepSeconds(3);
        // after split-brain healing, lock should still be be unlocked
        FencedLock secondLock = getBrains().getSecondHalf()[0].getCPSubsystem().getLock(LOCK_NAME);
        assertFalse("lock should be unlocked", secondLock.isLocked());
    }
}
