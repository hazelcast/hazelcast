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
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueService;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.test.starter.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

public class WipeDestroyedObjectsTest extends HazelcastRaftTestSupport {
    public static final Set<String> SERVICES = Set.of(LockService.SERVICE_NAME, SemaphoreService.SERVICE_NAME,
            CountDownLatchService.SERVICE_NAME, AtomicRefService.SERVICE_NAME, AtomicLongService.SERVICE_NAME);
    public static final Set<String> ATOMIC_SERVICES = Set.of(AtomicLongService.SERVICE_NAME, AtomicRefService.SERVICE_NAME);
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
        createUseDestroyWipeAssert(this::lockAcquireReleaseDestroy);
    }

    @Test
    public void testCountDownLatchDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createUseDestroyWipeAssert(this::cdlSetCountdownDestroy);
    }

    @Test
    public void testSemaphoreDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createUseDestroyWipeAssert(this::semaphoreAcquireReleaseDestroy);
    }

    @Test
    public void testAtomicReferenceDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createUseDestroyWipeAssert(this::atomicReferenceSetDestroy);
    }

    @Test
    public void testAtomicLongDestroyedNamesWiped()
            throws ExecutionException, InterruptedException {
        createUseDestroyWipeAssert(this::atomicLongSetDestroy);
    }

    /**
     * Asserts that the destroyed object names per-each service are wiped, for each CP member.
     */
    private void assertAllDestroyedObjectsWiped() throws ExecutionException, InterruptedException {
        Collection<CPGroupId> cpGroups =
                getCpSubsystem().getCPSubsystemManagementService().getCPGroupIds().toCompletableFuture().get();
        for (HazelcastInstance instance : instances) {
            for (String service : SERVICES) {
                for (CPGroupId cpGroupId : cpGroups) {
                    if (cpGroupId.getName().equals("METADATA")) {
                        continue;
                    }

                    assertTrueEventually(() -> {
                        if (ATOMIC_SERVICES.contains(service)) {
                            RaftAtomicValueService<?, ?, ?> raftAtomicValueService = getNodeEngineImpl(instance).getService(service);
                            Set<?> destroyedValues =
                                    ReflectionUtils.getFieldValueReflectively(raftAtomicValueService, "destroyedValues");
                            assertTrue(destroyedValues.isEmpty());
                        } else {
                            AbstractBlockingService<?, ?, ?> abstractBlockingService = getNodeEngineImpl(instance).getService(service);
                            ResourceRegistry<?, ?> resourceRegistry = abstractBlockingService.getRegistryOrNull(cpGroupId);
                            if (resourceRegistry != null) {
                                // this is thread-safe at the point of the method as the wipeDestroyedObjects blocks, so we should
                                // be the only thread reading (no writers should be present on this) this thread unsafe data
                                // structure
                                Set<String> destroyedNames =
                                        ReflectionUtils.getFieldValueReflectively(resourceRegistry, "destroyedNames");
                                assertTrue(destroyedNames.isEmpty());
                            }
                        }
                    });
                }
            }
        }
    }

    private void createDefaultAndCustomGroupObjects(Consumer<String> consumer) {
        for (String cpGroupName : CP_GROUP_NAMES) {
            consumer.accept(randomName() + (!cpGroupName.equals("default") ? ("@" + cpGroupName) : ""));
        }
    }

    private void createUseDestroyWipeAssert(Consumer<String> op)
            throws ExecutionException, InterruptedException {
        createDefaultAndCustomGroupObjects(op);
        getCpSubsystem().getCPSubsystemManagementService().wipeDestroyedObjects();
        assertAllDestroyedObjectsWiped();
    }

    private void cdlSetCountdownDestroy(String name) {
        ICountDownLatch cdl = getCpSubsystem().getCountDownLatch(name);
        assertTrue(cdl.trySetCount(1));
        try {
        } finally {
            cdl.countDown();
        }
        cdl.destroy();
    }

    private void lockAcquireReleaseDestroy(String name) {
        FencedLock lock = getCpSubsystem().getLock(name);
        lock.lock();
        try {
        } finally {
            lock.unlock();
        }
        lock.destroy();
    }

    private void semaphoreAcquireReleaseDestroy(String name) {
        ISemaphore semaphore = getCpSubsystem().getSemaphore(name);
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

    private void atomicReferenceSetDestroy(String name) {
        IAtomicReference<String> atomicReference = getCpSubsystem().getAtomicReference(name);
        atomicReference.set(randomName());
        atomicReference.destroy();
    }

    private void atomicLongSetDestroy(String name) {
        IAtomicLong atomicLong = getCpSubsystem().getAtomicLong(name);
        atomicLong.set(1);
        atomicLong.destroy();
    }
}
