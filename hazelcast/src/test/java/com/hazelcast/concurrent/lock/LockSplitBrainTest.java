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

package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockSplitBrainTest extends SplitBrainTestSupport {

    private String key;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);

        HazelcastInstance lastInstance = instances[instances.length - 1];
        key = generateKeyOwnedBy(lastInstance);

        ILock lock = lastInstance.getLock(key);
        lock.lock();

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        // acquire lock on 1st brain
        firstBrain[0].getLock(key).lock();

        // release lock on 2nd brain
        secondBrain[0].getLock(key).forceUnlock();
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // all instances observe lock as acquired
        for (HazelcastInstance instance : instances) {
            ILock lock = instance.getLock(key);
            assertTrue(lock.isLocked());
        }
    }
}
