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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueSplitBrainTest extends SplitBrainTestSupport {

    private String name = randomString();
    private final int initialCount = 100;
    private final int finalCount = initialCount + 50;

    @Override
    protected int[] brains() {
        // 2nd merges to the 1st
        return new int[]{2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        IQueue<Object> queue = instances[0].getQueue(name);

        for (int i = 0; i < initialCount; i++) {
            queue.offer("item" + i);
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        IQueue<Object> queue1 = firstBrain[0].getQueue(name);
        for (int i = initialCount; i < finalCount; i++) {
            queue1.offer("item" + i);
        }

        IQueue<Object> queue2 = secondBrain[0].getQueue(name);
        for (int i = initialCount; i < finalCount + 10; i++) {
            queue2.offer("lost-item" + i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        for (HazelcastInstance instance : instances) {
            IQueue<Object> queue = instance.getQueue(name);
            assertEquals(finalCount, queue.size());
        }

        IQueue<Object> queue = instances[instances.length - 1].getQueue(name);
        for (int i = 0; i < finalCount; i++) {
            assertEquals("item" + i, queue.poll());
        }
    }
}
