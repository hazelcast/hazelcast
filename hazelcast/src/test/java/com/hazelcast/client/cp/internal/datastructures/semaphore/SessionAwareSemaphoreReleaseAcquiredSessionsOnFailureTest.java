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

package com.hazelcast.client.cp.internal.datastructures.semaphore;

import com.hazelcast.client.cp.internal.session.ClientProxySessionManager;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests that acquire, tryAcquire and drainPermits methods
 * properly release the acquired sessions on errors
 * other than SessionExpiredException and WaitKeyCancelledException.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreReleaseAcquiredSessionsOnFailureTest extends HazelcastRaftTestSupport {

    private SessionAwareSemaphoreProxy semaphore;
    private ClientProxySessionManager sessionManager;
    private RaftGroupId groupId;

    @Before
    public void setup() {
        newInstances(3);
        String proxyName = "semaphore@group";
        HazelcastInstance client = ((TestHazelcastFactory) factory).newHazelcastClient();
        sessionManager = (((HazelcastClientProxy) client).client).getProxySessionManager();
        semaphore = (SessionAwareSemaphoreProxy) client.getCPSubsystem().getSemaphore(proxyName);
        groupId = (RaftGroupId) semaphore.getGroupId();
    }

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Test
    public void testAcquire_shouldReleaseSessionsOnRuntimeError() throws InterruptedException {
        initSemaphoreAndAcquirePermits(10, 5);
        assertEquals(getSessionAcquireCount(), 5);
        Future future = spawn(() -> {
            Thread.currentThread().interrupt();
            semaphore.acquire(5);
        });

        try {
            future.get();
            fail("Acquire request should have been completed with InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof HazelcastException);
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
        }
        assertEquals(getSessionAcquireCount(), 5);
    }

    @Test
    public void testTryAcquire_shouldReleaseSessionsOnRuntimeError() throws InterruptedException {
        initSemaphoreAndAcquirePermits(2, 1);
        assertEquals(getSessionAcquireCount(), 1);
        Future future = spawn(() -> {
            Thread.currentThread().interrupt();
            semaphore.tryAcquire(10, TimeUnit.MINUTES);
        });

        try {
            future.get();
            fail("TryAcquire request should have been completed with InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof HazelcastException);
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
        }
        assertEquals(getSessionAcquireCount(), 1);
    }

    @Test
    public void testDrainPermits_shouldReleaseSessionsOnRuntimeError() throws InterruptedException {
        initSemaphoreAndAcquirePermits(42, 2);
        assertEquals(getSessionAcquireCount(), 2);
        Future future = spawn(() -> {
            Thread.currentThread().interrupt();
            semaphore.drainPermits();
        });

        try {
            future.get();
            fail("DrainPermits request should have been completed with InterruptedException");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof HazelcastException);
            assertTrue(e.getCause().getCause() instanceof InterruptedException);
        }
        assertEquals(getSessionAcquireCount(), 2);
    }

    private void initSemaphoreAndAcquirePermits(int initialPermits, int acquiredPermits) {
        // Make sure that we have a session id initialized so that the further
        // requests to acquire sessions do not make a remote call.
        semaphore.init(initialPermits);
        semaphore.acquire(acquiredPermits);
    }

    private long getSessionAcquireCount() {
        long sessionId = sessionManager.getSession(groupId);
        assertNotEquals(sessionId, NO_SESSION_ID);
        return sessionManager.getSessionAcquireCount(groupId, sessionId);
    }
}
