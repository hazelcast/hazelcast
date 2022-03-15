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
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.lock.operation.UnlockOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.lock.FencedLock;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.datastructures.lock.FencedLockBasicTest.lockByOtherThread;
import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractFencedLockAdvancedTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance primaryInstance;
    protected HazelcastInstance proxyInstance;
    protected FencedLock lock;
    protected String objectName = "lock";

    @Before
    public void setup() {
        instances = createInstances();
        primaryInstance = getPrimaryInstance();
        proxyInstance = getProxyInstance();
        lock = proxyInstance.getCPSubsystem().getLock(getProxyName());
        assertNotNull(lock);
    }

    protected abstract String getProxyName();

    protected abstract HazelcastInstance[] createInstances();

    protected abstract HazelcastInstance getPrimaryInstance();

    protected HazelcastInstance getProxyInstance() {
        return getPrimaryInstance();
    }

    @Test
    public void testSuccessfulLockClearsWaitTimeouts() {
        lock.lock();

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            lock.lock();
            latch.countDown();
        });

        assertTrueEventually(() -> assertFalse(registry.getLiveOperations().isEmpty()));

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }


    @Test
    public void testSuccessfulTryLockClearsWaitTimeouts() {
        lock.lock();

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        CountDownLatch latch = new CountDownLatch(1);
        spawn(() -> {
            lock.tryLock(10, MINUTES);
            latch.countDown();
        });

        assertTrueEventually(() -> {
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertFalse(registry.getLiveOperations().isEmpty());
        });

        lock.unlock();

        assertOpenEventually(latch);

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testFailedTryLockClearsWaitTimeouts() {
        lockByOtherThread(lock);

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        long fence = lock.tryLockAndGetFence(1, TimeUnit.SECONDS);

        assertEquals(FencedLock.INVALID_FENCE, fence);
        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testDestroyClearsWaitTimeouts() {
        lockByOtherThread(lock);

        CPGroupId groupId = lock.getGroupId();
        HazelcastInstance leader = leaderInstanceOf(groupId);
        LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
        LockRegistry registry = service.getRegistryOrNull(groupId);

        spawn(() -> {
            lock.tryLock(10, MINUTES);
        });

        assertTrueEventually(() -> {
            assertFalse(registry.getWaitTimeouts().isEmpty());
            assertFalse(registry.getLiveOperations().isEmpty());
        });

        lock.destroy();

        assertTrue(registry.getWaitTimeouts().isEmpty());
        assertTrue(registry.getLiveOperations().isEmpty());
    }

    @Test
    public void testInactiveSessionsAreEventuallyClosed() throws ExecutionException, InterruptedException {
        lock.lock();

        RaftGroupId groupId = (RaftGroupId) lock.getGroupId();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(groupId).get().isEmpty());
            }
        });

        RaftSessionService sessionService = getNodeEngineImpl(primaryInstance).getService(RaftSessionService.SERVICE_NAME);
        long sessionId = sessionService.getAllSessions(groupId).get().iterator().next().id();

        getRaftInvocationManager(proxyInstance)
                .invoke(groupId, new UnlockOp(objectName, sessionId, getThreadId(), newUnsecureUUID())).joinInternal();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService service = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertTrue(service.getAllSessions(groupId).get().isEmpty());
            }

            ProxySessionManagerService service = getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
            assertEquals(NO_SESSION_ID, service.getSession(groupId));
        });
    }

    @Test
    public void testActiveSessionIsNotClosedWhenLockIsHeld() {
        lock.lock();

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(lock.getGroupId()).get().isEmpty());
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertFalse(sessionService.getAllSessions(lock.getGroupId()).get().isEmpty());
            }
        }, 20);
    }

    @Test
    public void testActiveSessionIsNotClosedWhenPendingWaitKey() {
        FencedLock other = null;
        for (HazelcastInstance instance : instances) {
            if (instance != proxyInstance) {
                other = instance.getCPSubsystem().getLock(lock.getName());
                break;
            }
        }

        assertNotNull(other);

        // lock from another instance
        other.lock();

        spawn(() -> {
            lock.tryLock(30, TimeUnit.MINUTES);
        });

        assertTrueEventually(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertEquals(2, sessionService.getAllSessions(lock.getGroupId()).get().size());
            }
        });

        assertTrueAllTheTime(() -> {
            for (HazelcastInstance instance : instances) {
                RaftSessionService sessionService = getNodeEngineImpl(instance).getService(RaftSessionService.SERVICE_NAME);
                assertEquals(2, sessionService.getAllSessions(lock.getGroupId()).get().size());
            }
        }, 20);
    }

    @Test
    public void testLockAcquired_whenLockOwnerShutsDown() {
        lock.lock();

        CountDownLatch remoteLockedLatch = new CountDownLatch(1);
        spawn(() -> {
            HazelcastInstance otherInstance = instances[0] == proxyInstance ? instances[1] : instances[0];
            FencedLock remoteLock = otherInstance.getCPSubsystem().getLock(lock.getName());
            remoteLock.lock();
            remoteLockedLatch.countDown();
        });

        assertTrueEventually(() -> {
            LockService service = getNodeEngineImpl(primaryInstance).getService(LockService.SERVICE_NAME);
            LockRegistry registry = service.getRegistryOrNull(this.lock.getGroupId());
            assertNotNull(registry);
            Lock lock = registry.getResourceOrNull(objectName);
            assertNotNull(lock);
            assertFalse(lock.getInternalWaitKeysMap().isEmpty());
        });

        proxyInstance.shutdown();

        assertOpenEventually(remoteLockedLatch);
    }

    protected abstract HazelcastInstance leaderInstanceOf(CPGroupId groupId);
}
