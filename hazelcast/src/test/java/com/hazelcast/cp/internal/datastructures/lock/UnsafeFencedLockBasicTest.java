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

package com.hazelcast.cp.internal.datastructures.lock;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeFencedLockBasicTest extends AbstractFencedLockBasicTest {

    @Override
    protected String getProxyName() {
        warmUpPartitions(instances);
        String group = generateKeyOwnedBy(primaryInstance);
        String objectName = "lock";
        return objectName + "@" + group;
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        return factory.newInstances(new Config(), 2);
    }

    @Override
    protected HazelcastInstance getPrimaryInstance() {
        return instances[0];
    }

    @Override
    protected HazelcastInstance getProxyInstance() {
        return instances[1];
    }

    @Test
    public void testPrimaryInstanceCrash() {
        lock.lock();
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().terminate();
        assertTrue(lock.isLocked());
    }

    @Test
    public void testPrimaryInstanceShutdown() {
        lock.lock();
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().shutdown();
        assertTrue(lock.isLocked());
    }

}
