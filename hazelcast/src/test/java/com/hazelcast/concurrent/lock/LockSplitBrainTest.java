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

package com.hazelcast.concurrent.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockSplitBrainTest extends HazelcastTestSupport {

    @Test
    public void testLockSplitBrain_acquireSameLock() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = newConfig();

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        HazelcastInstance h3 = factory.newHazelcastInstance(config);
        warmUpPartitions(h1, h2, h3);

        String key = generateKeyOwnedBy(h3);
        ILock lock1 = h1.getLock(key);
        ILock lock3 = h3.getLock(key);
        lock3.lock();

        waitAllForSafeState(h1, h2, h3);

        // create split: [h1, h2] & [h3]
        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertClusterSizeEventually(2, h1);
        assertClusterSizeEventually(2, h2);
        assertClusterSizeEventually(1, h3);

        // acquire lock on [h1, h2] side
        lock1.lock();

        // release lock on h3 side
        lock3.forceUnlock();

        // merge back
        getNode(h3).getClusterService().merge(getAddress(h1));

        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        // h3 observes lock as acquired
        assertTrue(lock3.isLocked());
    }


    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "600");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "600");
        return config;
    }
}
