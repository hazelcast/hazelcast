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
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeBoundedReentrantFencedLockTest extends AbstractBoundedReentrantFencedLockTest {

    @Override
    protected FencedLock createLock() {
        String proxyName = getProxyName();
        return instances[1].getCPSubsystem().getLock(proxyName);
    }

    protected final String getProxyName() {
        HazelcastInstance primaryInstance = instances[0];
        warmUpPartitions(instances);
        String group = generateKeyOwnedBy(primaryInstance);
        return objectName + "@" + group;
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        Config config = new Config();
        FencedLockConfig lockConfig = new FencedLockConfig(objectName, 2);
        config.getCPSubsystemConfig().addLockConfig(lockConfig);
        return factory.newInstances(config, 2);
    }
}
