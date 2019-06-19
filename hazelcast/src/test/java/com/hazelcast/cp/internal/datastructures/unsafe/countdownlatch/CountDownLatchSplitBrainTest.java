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

package com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CountDownLatchSplitBrainTest extends SplitBrainTestSupport {

    private String name;
    private int count = 5;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[]{2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
        name = generateKeyOwnedBy(instances[instances.length - 1]);

        ICountDownLatch latch = instances[0].getCountDownLatch(name);
        latch.trySetCount(count);

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        ICountDownLatch latch1 = firstBrain[0].getCountDownLatch(name);
        // count = 4
        latch1.countDown();
        count = latch1.getCount();

        ICountDownLatch latch2 = secondBrain[0].getCountDownLatch(name);
        // count = 0
        while (latch2.getCount() > 0) {
            latch2.countDown();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        for (HazelcastInstance instance : instances) {
            ICountDownLatch latch = instance.getCountDownLatch(name);
            assertEquals(count, latch.getCount());
        }
    }
}
