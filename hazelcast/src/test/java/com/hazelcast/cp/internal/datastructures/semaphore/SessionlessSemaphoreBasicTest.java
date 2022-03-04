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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.SemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionlessSemaphoreBasicTest extends AbstractSessionlessSemaphoreBasicTest {

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    @Override
    protected ISemaphore createSemaphore() {
        return instances[RandomPicker.getInt(instances.length)].getCPSubsystem().getSemaphore(objectName + "@group");
    }

    @Override
    protected HazelcastInstance leaderInstanceOf(CPGroupId groupId) {
        return getLeaderInstance(instances, groupId);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        SemaphoreConfig semaphoreConfig = new SemaphoreConfig(objectName, true, 0);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
