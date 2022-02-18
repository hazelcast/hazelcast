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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigSplitBrain_whenConfigExistsInSmallerBrainLiteMemberOnlyTest
        extends DynamicConfigSplitBrain_whenConfigExistsInSmallerBrainOnlyTest {

    // start 2 data members and 1 lite member
    @Override
    protected HazelcastInstance[] startInitialCluster(Config config, int clusterSize) {
        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[clusterSize];
        factory = createHazelcastInstanceFactory(clusterSize);
        for (int i = 0; i < clusterSize - 1; i++) {
            HazelcastInstance hz = factory.newHazelcastInstance(config);
            hazelcastInstances[i] = hz;
        }
        config.setLiteMember(true);
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        hazelcastInstances[clusterSize - 1] = hz;
        return hazelcastInstances;
    }
}
