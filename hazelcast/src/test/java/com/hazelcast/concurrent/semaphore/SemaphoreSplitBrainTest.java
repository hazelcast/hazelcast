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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
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
public class SemaphoreSplitBrainTest extends HazelcastTestSupport {

    @Test
    public void testSemaphoreSplitBrain() throws InterruptedException {
        Config config = newConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        final HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        HazelcastInstance h3 = factory.newHazelcastInstance(config);
        warmUpPartitions(h1, h2, h3);

        final String key = generateKeyOwnedBy(h3);
        final ISemaphore semaphore1 = h1.getSemaphore(key);
        final ISemaphore semaphore3 = h3.getSemaphore(key);
        semaphore1.init(5);
        semaphore3.acquire(3);
        assertEquals(2, semaphore3.availablePermits());

        waitAllForSafeState(h1, h2, h3);

        // create split: [h1, h2] & [h3]
        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(1, h3);

        // when member is down, permits are released.
        // since releasing the permits is async, we use assert eventually
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(5, semaphore1.availablePermits());
            }
        });

        semaphore1.acquire(4);

        // merge back
        getNode(h3).getClusterService().merge(getAddress(h1));

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        assertEquals(1, semaphore3.availablePermits());
    }

    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "600");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "600");
        return config;
    }
}
