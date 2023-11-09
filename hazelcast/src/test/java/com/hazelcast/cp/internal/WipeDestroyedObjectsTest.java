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

package com.hazelcast.cp.internal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WipeDestroyedObjectsTest extends HazelcastRaftTestSupport {
    private static final Set<String> CP_GROUP_NAMES = Set.of("mygroup1", "mygroup2", "default");
    private HazelcastInstance[] instances;

    private CPSubsystem getCpSubsystem() {
        return instances[0].getCPSubsystem();
    }

    @Before
    public void setup() {
        instances = newInstances(3);
    }

    @Test
    public void testFenceLockDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createWipeCreate(this::lockAcquireReleaseDestroy);
    }

    @Test
    public void testCountDownLatchDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createWipeCreate(this::cdlSetCountdownDestroy);
    }

    @Test
    public void testSemaphoreDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createWipeCreate(this::semaphoreAcquireReleaseDestroy);
    }

    @Test
    public void testAtomicReferenceDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createWipeCreate(this::atomicReferenceSetDestroy);
    }

    @Test
    public void testAtomicLongDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createWipeCreate(this::atomicLongSetDestroy);
    }

    private void createDefaultAndCustomGroupObjects(Consumer<String> consumer) {
        for (String cpGroupName : CP_GROUP_NAMES) {
            consumer.accept(!cpGroupName.equals("default") ? ("@" + cpGroupName) : "");
        }
    }

    private void createWipeCreate(Consumer<String> op) throws ExecutionException, InterruptedException {
        createDefaultAndCustomGroupObjects(op);
        getCpSubsystem().getCPSubsystemManagementService().wipeDestroyedObjects().toCompletableFuture().get();
        createDefaultAndCustomGroupObjects(op);
    }

    private void cdlSetCountdownDestroy(String cpGroup) {
        ICountDownLatch cdl = getCpSubsystem().getCountDownLatch("cdl" + cpGroup);
        assertNotNull(cdl);
        assertTrue(cdl.trySetCount(1));
        try {
        } finally {
            cdl.countDown();
        }
        cdl.destroy();
    }

    private void lockAcquireReleaseDestroy(String cpGroup) {
        FencedLock lock = getCpSubsystem().getLock("fencedlock" + cpGroup);
        assertNotNull(lock);
        lock.lock();
        try {
        } finally {
            lock.unlock();
        }
        lock.destroy();
    }

    private void semaphoreAcquireReleaseDestroy(String cpGroup) {
        ISemaphore semaphore = getCpSubsystem().getSemaphore("semaphore" + cpGroup);
        assertNotNull(semaphore);
        assertTrue(semaphore.init(1));
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
        } finally {
            semaphore.release();
        }
        semaphore.destroy();
    }

    private void atomicReferenceSetDestroy(String cpGroup) {
        IAtomicReference<String> atomicReference = getCpSubsystem().getAtomicReference("atomicref" + cpGroup);
        assertNotNull(atomicReference);
        atomicReference.set(randomName());
        atomicReference.destroy();
    }

    private void atomicLongSetDestroy(String cpGroup) {
        IAtomicLong atomicLong = getCpSubsystem().getAtomicLong("atomiclong" + cpGroup);
        assertNotNull(atomicLong);
        atomicLong.set(1);
        atomicLong.destroy();
    }
}
