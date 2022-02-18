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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
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
public class UnsafeAtomicLongBasicTest extends AbstractAtomicLongBasicTest {

    private HazelcastInstance primaryInstance;

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = factory.newInstances(new Config(), 2);
        primaryInstance = instances[0];
        return instances;
    }

    @Override
    protected String getName() {
        warmUpPartitions(instances);
        assertNotNull(primaryInstance);
        String group = generateKeyOwnedBy(primaryInstance);
        return "long@" + group;
    }

    @Override
    protected IAtomicLong createAtomicLong(String name) {
        HazelcastInstance instance = instances[1];
        return instance.getCPSubsystem().getAtomicLong(name);
    }

    @Test
    public void testPrimaryInstanceCrash() {
        atomicLong.set(111);
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().terminate();
        assertEquals(111, atomicLong.get());
    }

    @Test
    public void testPrimaryInstanceShutdown() {
        atomicLong.set(111);
        primaryInstance.getLifecycleService().shutdown();
        assertEquals(111, atomicLong.get());
    }
}
