/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeFencedLockAdvancedTest extends AbstractFencedLockAdvancedTest {

    @Override
    protected HazelcastInstance[] createInstances() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);
        return factory.newInstances(config, 2);
    }

    @Override
    protected HazelcastInstance getPrimaryInstance() {
        return instances[0];
    }

    @Override
    protected HazelcastInstance getProxyInstance() {
        return instances[1];
    }

    @Override
    protected String getProxyName() {
        warmUpPartitions(instances);
        String group = generateKeyOwnedBy(primaryInstance);
        return objectName + "@" + group;
    }

    @Override
    protected HazelcastInstance leaderInstanceOf(CPGroupId groupId) {
        return primaryInstance;
    }
}
