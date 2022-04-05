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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.session.AbstractProxySessionManager.NO_SESSION_ID;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.PacketFiltersUtil.dropOperationsFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * Tests that acquire, tryAcquire and drainPermits methods
 * properly release the acquired sessions on errors
 * other than SessionExpiredException and WaitKeyCancelledException.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreReleaseAcquiredSessionsOnFailureTest extends HazelcastRaftTestSupport {

    private HazelcastInstance proxyInstance;
    private ProxySessionManagerService sessionManagerService;
    private SessionAwareSemaphoreProxy semaphore;
    private RaftGroupId groupId;

    @Before
    public void setup() {
        HazelcastInstance[] instances = newInstances(3);
        String proxyName = "semaphore@group";
        SessionAwareProxy proxy = (SessionAwareProxy) instances[0].getCPSubsystem().getSemaphore(proxyName);
        proxyInstance = getRandomFollowerInstance(instances, proxy.getGroupId());
        semaphore = (SessionAwareSemaphoreProxy) proxyInstance.getCPSubsystem().getSemaphore(proxyName);
        groupId = semaphore.getGroupId();
        sessionManagerService = getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "5000");
        return config;
    }

    @Test
    public void testAcquire_shouldReleaseSessionsOnRuntimeError() {
        initSemaphoreAndAcquirePermits(10, 5);
        assertEquals(getSessionAcquireCount(), 5);
        dropReplicateOperations();
        try {
            semaphore.acquire(5);
            fail("Acquire operation should have been dropped and failed with OperationTimeoutException");
        } catch (OperationTimeoutException ignored) {
        }
        assertEquals(getSessionAcquireCount(), 5);
    }


    @Test
    public void testTryAcquire_shouldReleaseSessionsOnRuntimeError() {
        initSemaphoreAndAcquirePermits(2, 1);
        assertEquals(getSessionAcquireCount(), 1);
        dropReplicateOperations();
        try {
            semaphore.tryAcquire(10, TimeUnit.MINUTES);
            fail("Acquire operation should have been dropped and failed with OperationTimeoutException");
        } catch (OperationTimeoutException ignored) {
        }
        assertEquals(getSessionAcquireCount(), 1);
    }

    @Test
    public void testDrainPermits_shouldReleaseSessionsOnRuntimeError() {
        initSemaphoreAndAcquirePermits(42, 2);
        assertEquals(getSessionAcquireCount(), 2);
        dropReplicateOperations();
        try {
            semaphore.drainPermits();
            fail("DrainPermits operation should have been dropped and failed with OperationTimeoutException");
        } catch (OperationTimeoutException ignored) {
        }
        assertEquals(getSessionAcquireCount(), 2);
    }

    private void initSemaphoreAndAcquirePermits(int initialPermits, int acquiredPermits) {
        // Make sure that we have a session id initialized so that the further
        // requests to acquire sessions do not make a remote call.
        semaphore.init(initialPermits);
        semaphore.acquire(acquiredPermits);
    }

    private void dropReplicateOperations() {
        List<Integer> opTypes = Collections.singletonList(RaftServiceDataSerializerHook.DEFAULT_RAFT_GROUP_REPLICATE_OP);
        dropOperationsFrom(proxyInstance, RaftServiceDataSerializerHook.F_ID, opTypes);
    }

    private long getSessionAcquireCount() {
        long sessionId = sessionManagerService.getSession(groupId);
        assertNotEquals(sessionId, NO_SESSION_ID);
        return sessionManagerService.getSessionAcquireCount(groupId, sessionId);
    }
}
