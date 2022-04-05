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
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BoundedReentrantFencedLockTest extends AbstractBoundedReentrantFencedLockTest {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance lockInstance;
    protected FencedLock lock;

    @Override
    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = newInstances(3);
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return instances;
    }

    @Override
    protected FencedLock createLock() {
        return lockInstance.getCPSubsystem().getLock(objectName + "@group");
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        FencedLockConfig lockConfig = new FencedLockConfig(objectName, 2);
        config.getCPSubsystemConfig().addLockConfig(lockConfig);
        return config;
    }

}
