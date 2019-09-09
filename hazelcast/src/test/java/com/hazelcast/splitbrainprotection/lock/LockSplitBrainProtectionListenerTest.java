/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.splitbrainprotection.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.ILock;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionListenerTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LockSplitBrainProtectionListenerTest extends AbstractSplitBrainProtectionListenerTest {

    @Override
    protected void addSplitBrainProtectionConfig(Config config, String distributedObjectName, String splitBrainProtectionName) {
        config.getLockConfig(distributedObjectName).setSplitBrainProtectionName(splitBrainProtectionName);
    }

    @Test
    public void testSplitBrainProtectionFailureEventFiredWhenNodeCountBelowThreshold() {
        CountDownLatch splitBrainProtectionNotPresent = new CountDownLatch(1);
        String lockName = randomString();
        Config config = addSplitBrainProtection(new Config(), lockName,
                splitBrainProtectionListener(null, splitBrainProtectionNotPresent));
        HazelcastInstance instance = createHazelcastInstance(config);
        ILock q = instance.getLock(lockName);
        try {
            q.lock();
        } catch (Exception expected) {
            expected.printStackTrace();
        }
        assertOpenEventually(splitBrainProtectionNotPresent, 15);
    }
}
