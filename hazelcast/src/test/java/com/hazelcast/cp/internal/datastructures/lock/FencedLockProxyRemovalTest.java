/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.lock.proxy.FencedLockProxy;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockProxyRemovalTest extends HazelcastRaftTestSupport {
    protected HazelcastInstance[] instances;

    @Before
    public void setup() {
        instances = newInstances(3);
    }

    @Test
    public void testDestroy_AllMembersRemoveLockProxyDefaultCpGroup() {
        allMembersRemoveLockProxy(randomName());
    }

    @Test
    public void testDestroy_AllMembersRemoveLockProxyCustomCpGroup() {
        allMembersRemoveLockProxy(randomName() + "@mygroup");
    }

    private void allMembersRemoveLockProxy(String lockName) {
        FencedLock myLockMember0 = instances[0].getCPSubsystem().getLock(lockName);
        FencedLock myLockMember1 = instances[1].getCPSubsystem().getLock(lockName);
        FencedLock myLockMember2 = instances[2].getCPSubsystem().getLock(lockName);

        myLockMember0.lock();
        try {
        } finally {
            myLockMember0.unlock();
        }
        myLockMember0.destroy();

        for (HazelcastInstance instance : instances) {
            assertTrueEventually(() -> {
                LockService service = getNodeEngineImpl(instance).getService(LockService.SERVICE_NAME);
                ConcurrentMap<String, FencedLockProxy> proxies = ReflectionUtils.getFieldValueReflectively(service, "proxies");
                assertTrue(proxies.isEmpty());
            });
        }
    }
}
