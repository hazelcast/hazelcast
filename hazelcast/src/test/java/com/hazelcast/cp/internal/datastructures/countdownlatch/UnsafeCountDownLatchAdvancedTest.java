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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeCountDownLatchAdvancedTest extends AbstractCountDownLatchAdvancedTest {

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

    @Override
    protected HazelcastInstance leaderInstanceOf(CPGroupId groupId) {
        return primaryInstance;
    }
}
