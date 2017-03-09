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

package com.hazelcast.concurrent.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CountDownLatchSplitBrainTest extends HazelcastTestSupport {

    @Test
    public void testCountDownLatchSplitBrain() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = newConfig();

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        HazelcastInstance h3 = factory.newHazelcastInstance(config);
        warmUpPartitions(h1, h2, h3);

        String name = generateKeyOwnedBy(h3);
        ICountDownLatch countDownLatch1 = h1.getCountDownLatch(name);
        ICountDownLatch countDownLatch3 = h3.getCountDownLatch(name);
        countDownLatch3.trySetCount(5);

        waitAllForSafeState(h1, h2, h3);

        // create split: [h1, h2] & [h3]
        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(1, h3);

        // modify both latches after split with different counts

        // count of h1 & h2 = 4
        countDownLatch1.countDown();

        // count of h3 = 0
        while (countDownLatch3.getCount() > 0) {
            countDownLatch3.countDown();
        }

        // merge back
        getNode(h3).getClusterService().merge(getAddress(h1));

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        // latch count should be equal to the count of larger cluster
        assertEquals(4, countDownLatch3.getCount());
    }

    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "600");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "600");
        return config;
    }
}
