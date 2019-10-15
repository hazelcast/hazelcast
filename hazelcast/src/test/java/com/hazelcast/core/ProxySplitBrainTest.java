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

package com.hazelcast.core;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProxySplitBrainTest extends SplitBrainTestSupport {

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        HazelcastInstance isolatedInstance = secondBrain[0];
        isolatedInstance.getQueue("isolatedQ");
        assertDistributedObjectCountEventually(1, isolatedInstance);

        for (HazelcastInstance hz : firstBrain) {
            String name = generateKeyOwnedBy(hz);
            hz.getQueue(name);
        }

        for (HazelcastInstance hz : firstBrain) {
            int expectedCount = firstBrain.length;
            assertDistributedObjectCountEventually(expectedCount, hz);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] allInstances) {
        for (HazelcastInstance hz : allInstances) {
            assertDistributedObjectCountEventually(allInstances.length, hz);
        }
    }

    private static void assertDistributedObjectCountEventually(final int expectedCount, final HazelcastInstance hz) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int actualSize = hz.getDistributedObjects().size();
                assertEquals(expectedCount, actualSize);
            }
        });
    }
}
