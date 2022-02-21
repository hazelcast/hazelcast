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
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.exception.WaitKeyCancelledException;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.cp.internal.datastructures.semaphore.operation.DrainPermitsOp;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.cp.internal.session.SessionAwareProxy;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public abstract class AbstractSemaphoreFailureTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;
    protected HazelcastInstance primaryInstance;
    protected HazelcastInstance proxyInstance;
    protected ProxySessionManagerService sessionManagerService;
    protected ISemaphore semaphore;
    protected String objectName = "semaphore";

    @Before
    public void setup() {
        instances = createInstances();
        primaryInstance = getPrimaryInstance();
        proxyInstance = getProxyInstance();
        semaphore = proxyInstance.getCPSubsystem().getSemaphore(getProxyName());
        sessionManagerService = getNodeEngineImpl(proxyInstance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    protected abstract HazelcastInstance[] createInstances();

    protected abstract String getProxyName();

    protected abstract HazelcastInstance getPrimaryInstance();

    protected HazelcastInstance getProxyInstance() {
        return getPrimaryInstance();
    }

    abstract boolean isJDKCompatible();

    private RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((SessionAwareProxy) semaphore).getGroupId();
    }

    abstract long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId);

    long getThreadId(RaftGroupId groupId) {
        return sessionManagerService.getOrCreateUniqueThreadId(groupId);
    }

    @Test(timeout = 300_000)
    public void testRetriedAcquireDoesNotCancelPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, -1));

        assertTrueAllTheTime(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertEquals(1, registry.getWaitTimeouts().size());
        }, 10);
    }

    @Test(timeout = 300_000)
    public void testNewAcquireCancelsPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, -1));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewAcquireCancelsPendingAcquireRequestWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, -1));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testTryAcquireWithTimeoutCancelsPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 100));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewTryAcquireWithTimeoutCancelsPendingAcquireRequestWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 100));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewTryAcquireWithoutTimeoutCancelsPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 0));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testNewTryAcquireWithoutTimeoutCancelsPendingAcquireRequestsWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid2, 1, 0));

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testReleaseCancelsPendingAcquireRequestWhenPermitsAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        try {
            semaphore.release();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testReleaseCancelsPendingAcquireRequestWhenNoPermitsAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        try {
            semaphore.release();
        } catch (IllegalStateException ignored) {
        }

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testDrainCancelsPendingAcquireRequestWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        semaphore.drainPermits();

        try {
            f.joinInternal();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 300_000)
    public void testRetriedAcquireReceivesPermitsOnlyOnce() throws InterruptedException, ExecutionException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid1 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Object> f1 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertEquals(1, registry.getWaitTimeouts().size());
        });

        spawn(() -> {
            try {
                semaphore.tryAcquire(20, 5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            assertEquals(2, registry.getWaitTimeouts().size());
        });

        InternalCompletableFuture<Object> f2 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(objectName, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(() -> {
            SemaphoreService service = getNodeEngineImpl(primaryInstance).getService(SemaphoreService.SERVICE_NAME);
            SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
            Semaphore semaphore = registry.getResourceOrNull(objectName);
            assertEquals(2, semaphore.getInternalWaitKeysMap().size());
        });

        spawn(() -> semaphore.increasePermits(3)).get();

        f1.joinInternal();
        f2.joinInternal();

        assertEquals(2, semaphore.availablePermits());
    }

    @Test(timeout = 300_000)
    public void testExpiredAndRetriedTryAcquireRequestReceivesFailureResponse() throws InterruptedException, ExecutionException {
        assumeFalse(isJDKCompatible());

        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Boolean> f1 = invocationManager.invoke(groupId,
                new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, SECONDS.toMillis(5)));

        assertFalse(f1.joinInternal());

        spawn(() -> semaphore.release()).get();

        InternalCompletableFuture<Boolean> f2 = invocationManager.invoke(groupId,
                new AcquirePermitsOp(objectName, sessionId, threadId, invUid, 1, SECONDS.toMillis(5)));

        assertFalse(f2.joinInternal());
    }

    @Test(timeout = 300_000)
    public void testRetriedDrainRequestIsNotProcessedAgain() throws InterruptedException, ExecutionException {
        assumeFalse(isJDKCompatible());

        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(proxyInstance, groupId);
        long threadId = getThreadId(groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(proxyInstance);

        InternalCompletableFuture<Integer> f1 = invocationManager
                .invoke(groupId, new DrainPermitsOp(objectName, sessionId, threadId, invUid));

        assertEquals(0, (int) f1.joinInternal());

        spawn(() -> semaphore.release()).get();

        InternalCompletableFuture<Integer> f2 = invocationManager
                .invoke(groupId, new DrainPermitsOp(objectName, sessionId, threadId, invUid));

        assertEquals(0, (int) f2.joinInternal());
    }

    @Test
    public void testAcquireOnMultipleProxies() {
        HazelcastInstance otherInstance = instances[0] == proxyInstance ? instances[1] : instances[0];
        ISemaphore semaphore2 = otherInstance.getCPSubsystem().getSemaphore(semaphore.getName());

        semaphore.init(1);
        semaphore.tryAcquire(1);

        assertFalse(semaphore2.tryAcquire());
    }

}
