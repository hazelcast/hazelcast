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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionlessSemaphoreProxy;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractSessionlessSemaphoreBasicTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected ISemaphore semaphore;
    protected String objectName = "semaphore";

    @Before
    public void setup() {
        instances = createInstances();
        semaphore = createSemaphore();
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract ISemaphore createSemaphore();

    @Test(expected = IllegalArgumentException.class)
    public void testCreateProxyOnMetadataCPGroup() {
        instances[0].getCPSubsystem().getSemaphore(objectName + "@" + CPGroup.METADATA_CP_GROUP_NAME);
    }

    @Test
    public void testInit() {
        assertTrue(semaphore.init(7));
        assertEquals(7, semaphore.availablePermits());
    }

    @Test
    public void testInitFails_whenAlreadyInitialized() {
        assertTrue(semaphore.init(7));
        assertFalse(semaphore.init(5));
        assertEquals(7, semaphore.availablePermits());
    }

    @Test
    public void testAcquire() throws InterruptedException {
        int numberOfPermits = 20;
        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.acquire();
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testAcquire_whenNoPermits() {
        semaphore.init(0);
        Future future = spawn(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueAllTheTime(() -> {
            assertFalse(future.isDone());
            assertEquals(0, semaphore.availablePermits());
        }, 5);
    }

    @Test
    public void testAcquire_whenNoPermits_andSemaphoreDestroyed() throws Exception {
        semaphore.init(0);
        Future future = spawn(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        semaphore.destroy();
        try {
            future.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRelease() {
        int numberOfPermits = 20;
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(i, semaphore.availablePermits());
            semaphore.release();
        }

        assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test
    public void testAllowNegativePermits() {
        assertTrue(semaphore.init(10));

        semaphore.reducePermits(15);

        assertEquals(-5, semaphore.availablePermits());

        semaphore.release(10);

        assertEquals(5, semaphore.availablePermits());
    }

    @Test
    public void testNegativePermitsJucCompatibility() {
        assertTrue(semaphore.init(0));

        semaphore.reducePermits(100);
        semaphore.release(10);

        assertEquals(-90, semaphore.availablePermits());
        assertEquals(-90, semaphore.drainPermits());

        semaphore.release(10);

        assertEquals(10, semaphore.availablePermits());
        assertEquals(10, semaphore.drainPermits());
    }

    @Test
    public void testIncreasePermits() {
        assertTrue(semaphore.init(10));

        assertEquals(10, semaphore.availablePermits());

        semaphore.increasePermits(100);

        assertEquals(110, semaphore.availablePermits());
    }

    @Test
    public void testRelease_whenArgumentNegative() {
        try {
            semaphore.release(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testRelease_whenBlockedAcquireThread() {
        semaphore.init(0);

        new Thread(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        semaphore.release();

        assertTrueEventually(() -> assertEquals(0, semaphore.availablePermits()));
    }

    @Test
    public void testMultipleAcquire() throws InterruptedException {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.acquire(5);
        }
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testMultipleAcquire_whenNegative() throws InterruptedException {
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        try {
            semaphore.acquire(-5);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
        assertEquals(10, semaphore.availablePermits());

    }

    @Test
    public void testMultipleAcquire_whenNotEnoughPermits() {
        int numberOfPermits = 5;
        semaphore.init(numberOfPermits);

        Thread thread = new Thread(() -> {
            try {
                semaphore.acquire(6);
                assertEquals(5, semaphore.availablePermits());
                semaphore.acquire(6);
                assertEquals(5, semaphore.availablePermits());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.start();

        assertTrueAllTheTime(() -> {
            assertTrue(thread.isAlive());
            assertEquals(5, semaphore.availablePermits());
        }, 5);
    }

    @Test
    public void testMultipleRelease() {
        int numberOfPermits = 20;

        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(i, semaphore.availablePermits());
            semaphore.release(5);
        }
        assertEquals(semaphore.availablePermits(), numberOfPermits);
    }

    @Test
    public void testMultipleRelease_whenNegative() {
        semaphore.init(0);

        try {
            semaphore.release(-5);
            fail();

        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testMultipleRelease_whenBlockedAcquireThreads() throws Exception {
        int permits = 10;
        semaphore.init(permits);
        semaphore.acquire(permits);

        Future future = spawn(() -> {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        semaphore.release();
        future.get();
    }

    @Test
    public void testDrain() throws InterruptedException {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        semaphore.acquire(5);
        int drainedPermits = semaphore.drainPermits();
        assertEquals(drainedPermits, numberOfPermits - 5);
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testDrain_whenNoPermits() {
        semaphore.init(0);
        assertEquals(0, semaphore.drainPermits());
    }

    @Test
    public void testReduce() {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            semaphore.reducePermits(5);
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testReduce_whenArgumentNegative() {
        try {
            semaphore.reducePermits(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testIncrease_whenArgumentNegative() {
        try {
            semaphore.increasePermits(-5);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testTryAcquire() {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i++) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertTrue(semaphore.tryAcquire());
        }
        assertFalse(semaphore.tryAcquire());
        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testTryAcquireMultiple() {
        int numberOfPermits = 20;

        assertTrue(semaphore.init(numberOfPermits));
        for (int i = 0; i < numberOfPermits; i += 5) {
            assertEquals(numberOfPermits - i, semaphore.availablePermits());
            assertTrue(semaphore.tryAcquire(5));
        }

        assertEquals(semaphore.availablePermits(), 0);
    }

    @Test
    public void testTryAcquireMultiple_whenArgumentNegative() {
        int negativePermits = -5;
        semaphore.init(0);
        try {
            semaphore.tryAcquire(negativePermits);
            fail();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testTryAcquire_whenNotEnoughPermits() throws InterruptedException {
        int numberOfPermits = 10;
        semaphore.init(numberOfPermits);
        semaphore.acquire(10);
        boolean result = semaphore.tryAcquire(1);

        assertFalse(result);
        assertEquals(0, semaphore.availablePermits());
    }

    @Test
    public void testInit_whenNotInitialized() {
        boolean result = semaphore.init(2);

        assertTrue(result);
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testInit_whenAlreadyInitialized() {
        semaphore.init(2);

        boolean result = semaphore.init(4);

        assertFalse(result);
        assertEquals(2, semaphore.availablePermits());
    }

    @Test
    public void testIncreasePermits_notifiesPendingAcquires() {
        semaphore.init(1);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            try {
                semaphore.tryAcquire(2, 10, TimeUnit.MINUTES);
                latch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            CPGroupId groupId = getGroupId(semaphore);
            HazelcastInstance leader = leaderInstanceOf(groupId);
            SemaphoreService service = getNodeEngineImpl(leader).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        semaphore.increasePermits(1);

        assertOpenEventually(latch);
    }

    protected abstract HazelcastInstance leaderInstanceOf(CPGroupId groupId);

    @Test(expected = DistributedObjectDestroyedException.class)
    public void test_destroy() {
        semaphore.destroy();

        semaphore.init(1);
    }

    protected CPGroupId getGroupId(ISemaphore semaphore) {
        return ((SessionlessSemaphoreProxy) semaphore).getGroupId();
    }
}
