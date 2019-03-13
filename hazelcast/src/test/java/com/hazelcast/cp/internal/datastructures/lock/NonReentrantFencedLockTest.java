/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.assertInvalidFence;
import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.assertValidFence;
import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.lockByOtherThread;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NonReentrantFencedLockTest extends HazelcastRaftTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected HazelcastInstance[] instances;
    protected HazelcastInstance lockInstance;
    protected FencedLock lock;
    private final String objectName = "lock";

    @Before
    public void setup() {
        instances = createInstances();
        lock = lockInstance.getCPSubsystem().getLock(objectName + "@group1");
        assertNotNull(lock);
    }

    protected HazelcastInstance[] createInstances() {
        HazelcastInstance[] instances = newInstances(3);
        lockInstance = instances[RandomPicker.getInt(instances.length)];
        return instances;
    }

    @Test
    public void testLock() {
        lock.lock();
        lock.unlock();
    }

    @Test
    public void testLockAndGetFence() {
        long fence = lock.lockAndGetFence();
        assertValidFence(fence);

        lock.unlock();
    }

    @Test
    public void testTryLock() {
        boolean locked = lock.tryLock();
        assertTrue(locked);

        lock.unlock();
    }

    @Test
    public void testTryLockAndGetFence() {
        long fence = lock.tryLockAndGetFence();
        assertValidFence(fence);

        lock.unlock();
    }

    @Test
    public void testTryLockTimeout() {
        boolean locked = lock.tryLock(1, TimeUnit.SECONDS);
        assertTrue(locked);

        lock.unlock();
    }

    @Test
    public void testTryLockAndGetFenceTimeout() {
        long fence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);
        assertValidFence(fence);

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

        expectedException.expect(LockAcquireLimitReachedException.class);
        lock.lock();
    }

    @Test
    public void testReentrantTryLockFails() {
        lock.lock();
        long fence = lock.getFence();
        assertValidFence(fence);

        boolean locked = lock.tryLock();
        assertFalse(locked);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertEquals(fence, lock.getFence());
    }

    @Test
    public void testReentrantTryLockAndGetFenceFails() {
        lock.lock();
        long fence1 = lock.getFence();
        assertValidFence(fence1);

        long fence2 = lock.tryLockAndGetFence();
        assertInvalidFence(fence2);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertEquals(fence1, lock.getFence());
    }

    @Test
    public void testReentrantTryLockAndGetFenceWithTimeoutFails() {
        lock.lock();
        long fence1 = lock.getFence();
        assertValidFence(fence1);

        long fence2 = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);
        assertInvalidFence(fence2);
        assertTrue(lock.isLockedByCurrentThread());
        assertEquals(1, lock.getLockCount());
        assertEquals(fence1, lock.getFence());
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        FencedLockConfig lockConfig = new FencedLockConfig(objectName, 1);
        config.getCPSubsystemConfig().addLockConfig(lockConfig);
        return config;
    }

}
