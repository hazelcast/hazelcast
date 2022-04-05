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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RandomPicker;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockBasicTest extends AbstractFencedLockBasicTest {

    @Override
    protected HazelcastInstance getPrimaryInstance() {
        return instances[RandomPicker.getInt(instances.length)];
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    @Override
    protected String getProxyName() {
        return "lock@group";
    }

    @Test
    public void test_lockInterruptibly() throws InterruptedException {
        HazelcastInstance newInstance = factory.newHazelcastInstance(createConfig(3, 3));
        FencedLock lock = newInstance.getCPSubsystem().getLock("lock@group1");
        lock.lockInterruptibly();
        lock.unlock();

        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().terminate();
        }

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> ref = new AtomicReference<>();

        Thread thread = new Thread(() -> {
            try {
                latch.countDown();
                lock.lockInterruptibly();
            } catch (Throwable t) {
                ref.set(t);
            }
        });
        thread.start();

        assertOpenEventually(latch);

        thread.interrupt();

        assertTrueEventually(() -> {
            Throwable t = ref.get();
            assertTrue(t instanceof InterruptedException);
        });
    }

    @Test
    public void test_lockFailsAfterCPGroupDestroyed() throws ExecutionException, InterruptedException {
        instances[0].getCPSubsystem()
                    .getCPSubsystemManagementService()
                    .forceDestroyCPGroup(lock.getGroupId().getName())
                    .toCompletableFuture()
                    .get();

        try {
            lock.lock();
            fail();
        } catch (CPGroupDestroyedException ignored) {
        }

        proxyInstance.getCPSubsystem().getLock(lock.getName());
    }
}
