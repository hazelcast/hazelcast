/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeCountDownLatchBasicTest extends AbstractCountDownLatchBasicTest {

    private HazelcastInstance primaryInstance;

    @Override
    protected String getName() {
        assertNotNull(primaryInstance);
        warmUpPartitions(instances);
        String group = generateKeyOwnedBy(primaryInstance);
        String objectName = "latch";
        return objectName + "@" + group;
    }

    @Override
    protected ICountDownLatch createLatch(String proxyName) {
        return instances[1].getCPSubsystem().getCountDownLatch(proxyName);
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = factory.newInstances(new Config(), 2);
        primaryInstance = instances[0];
        return instances;
    }

    @Test
    public void testPrimaryInstanceCrash() {
        latch.trySetCount(11);
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().terminate();
        assertEquals(11, latch.getCount());
    }

    @Test
    public void testPrimaryInstanceShutdown() {
        latch.trySetCount(11);
        primaryInstance.getLifecycleService().shutdown();
        assertEquals(11, latch.getCount());
    }
}
