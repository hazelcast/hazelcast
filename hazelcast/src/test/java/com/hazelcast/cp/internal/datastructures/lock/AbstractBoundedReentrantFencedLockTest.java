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

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.FencedLockConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.lock.exception.LockAcquireLimitReachedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.assertInvalidFence;
import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.assertValidFence;
import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.lockByOtherThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractBoundedReentrantFencedLockTest extends HazelcastRaftTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected HazelcastInstance[] instances;
    protected FencedLock lock;
    protected final String objectName = "lock";

    @Before
    public void setup() {
        instances = createInstances();
        lock = createLock();
        assertNotNull(lock);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract FencedLock createLock();

    @Test
    public void testLock() {
        lock.lock();
        lock.lock();
        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testLockAndGetFence() {
        long fence1 = lock.lockAndGetFence();
        long fence2 = lock.lockAndGetFence();
        assertValidFence(fence1);
        assertEquals(fence1, fence2);
        assertEquals(fence1, lock.getFence());

        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testTryLock() {
        boolean locked1 = lock.tryLock();
        boolean locked2 = lock.tryLock();
        assertTrue(locked1);
        assertTrue(locked2);

        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testTryLockAndGetFence() {
        long fence1 = lock.tryLockAndGetFence();
        long fence2 = lock.tryLockAndGetFence();
        assertValidFence(fence1);
        assertEquals(fence1, fence2);
        assertEquals(fence1, lock.getFence());

        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testTryLockTimeout() {
        boolean locked1 = lock.tryLock(1, TimeUnit.SECONDS);
        boolean locked2 = lock.tryLock(1, TimeUnit.SECONDS);
        assertTrue(locked1);
        assertTrue(locked2);

        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testTryLockAndGetFenceTimeout() {
        long fence1 = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);
        long fence2 = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);
        assertValidFence(fence1);
        assertEquals(fence1, fence2);
        assertEquals(fence1, lock.getFence());

        lock.unlock();
        lock.unlock();
    }

    @Test
    public void testTryLockWhileLockedByAnotherEndpoint() {
        lockByOtherThread(lock);
        boolean locked = lock.tryLock();

        assertFalse(locked);
    }

    @Test
    public void testTryLockTimeoutWhileLockedByAnotherEndpoint() {
        lockByOtherThread(lock);

        boolean locked = lock.tryLock(1, TimeUnit.SECONDS);
        assertFalse(locked);
    }

    @Test
    public void testReentrantLockFails() {
        lock.lock();
        lock.lock();

        expectedException.expect(LockAcquireLimitReachedException.class);
        lock.lock();
    }

    @Test
    public void testReentrantTryLockFails() {
        lock.lock();
        lock.lock();

        boolean locked = lock.tryLock();
        assertFalse(locked);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
        assertValidFence(lock.getFence());
    }

    @Test
    public void testReentrantTryLockAndGetFenceFails() {
        lock.lock();
        lock.lock();
        long fence1 = lock.getFence();

        long fence2 = lock.tryLockAndGetFence();
        assertInvalidFence(fence2);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
        assertEquals(fence1, lock.getFence());
    }

    @Test
    public void testReentrantTryLockAndGetFenceWithTimeoutFails() {
        lock.lock();
        lock.lock();
        long fence1 = lock.getFence();

        long fence2 = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);
        assertInvalidFence(fence2);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(2, lock.getLockCount());
        assertEquals(fence1, lock.getFence());
    }

    @Test
    public void testReentrantLock_afterLockIsReleasedByAnotherEndpoint() {
        lock.lock();

        CountDownLatch latch = new CountDownLatch(1);

        spawn(() -> {
            lock.lock();
            lock.lock();

            lock.unlock();
            lock.unlock();
            latch.countDown();
        });

        lock.unlock();
        assertOpenEventually(latch);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        FencedLockConfig lockConfig = new FencedLockConfig(objectName, 2);
        config.getCPSubsystemConfig().addLockConfig(lockConfig);
        return config;
    }

}
