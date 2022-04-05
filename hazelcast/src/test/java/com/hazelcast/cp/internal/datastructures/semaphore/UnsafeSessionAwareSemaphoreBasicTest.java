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
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.operation.unsafe.UnsafeRaftReplicateOp;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeSessionAwareSemaphoreBasicTest extends AbstractSessionAwareSemaphoreBasicTest {

    protected HazelcastInstance primaryInstance;

    @Override
    protected String getProxyName() {
        primaryInstance = instances[0];
        String group = generateKeyOwnedBy(primaryInstance);
        return objectName + "@" + group;
    }

    @Override
    protected HazelcastInstance getProxyInstance() {
        return instances[1];
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        Config config = new Config();
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig(objectName);
        semaphoreConfig.setName(objectName);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return factory.newInstances(config, 2);
    }

    @Override
    protected <T> InternalCompletableFuture<T> invokeRaftOp(RaftGroupId groupId, RaftOp raftOp) {
        RaftService service = getNodeEngineImpl(instances[1]).getService(RaftService.SERVICE_NAME);
        return service.getInvocationManager().invokeOnPartition(new UnsafeRaftReplicateOp(groupId, raftOp));
    }

    @Test
    public void testInitNotifiesWaitingAcquires() {
        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            try {
                semaphore.tryAcquire(30, MINUTES);
                latch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(getGroupId(semaphore));
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        boolean success = semaphore.init(1);
        assertTrue(success);

        assertOpenEventually(latch);

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(getGroupId(semaphore));
            assertTrue(registry.getWaitTimeouts().isEmpty());
        });
    }

    @Test
    public void testPrimaryInstanceCrash() {
        semaphore.init(11);
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().terminate();
        assertEquals(11, semaphore.availablePermits());
    }

    @Test
    public void testPrimaryInstanceShutdown() {
        semaphore.init(11);
        primaryInstance.getLifecycleService().shutdown();
        assertEquals(11, semaphore.availablePermits());
    }
}
