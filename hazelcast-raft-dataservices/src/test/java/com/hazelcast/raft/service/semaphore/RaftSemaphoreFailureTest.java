package com.hazelcast.raft.service.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.config.raft.RaftSemaphoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.raft.impl.service.RaftInvocationManager;
import com.hazelcast.raft.service.exception.WaitKeyCancelledException;
import com.hazelcast.raft.service.semaphore.operation.AcquirePermitsOp;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.test.AssertTask;
import com.hazelcast.util.RandomPicker;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static com.hazelcast.util.UuidUtil.newUnsecureUUID;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class RaftSemaphoreFailureTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private HazelcastInstance semaphoreInstance;
    private ISemaphore semaphore;
    private String name = "semaphore";

    @Before
    public void setup() {
        instances = createInstances();
        semaphore = createSemaphore(name);
        assertNotNull(semaphore);
    }

    private HazelcastInstance[] createInstances() {
        return newInstances(3);
    }

    private ISemaphore createSemaphore(String name) {
        semaphoreInstance = instances[RandomPicker.getInt(instances.length)];
        return create(semaphoreInstance, RaftSemaphoreService.SERVICE_NAME, name);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        RaftSemaphoreConfig semaphoreConfig = new RaftSemaphoreConfig(name, isStrictModeEnabled());
        config.addRaftSemaphoreConfig(semaphoreConfig);
        return config;
    }

    abstract boolean isStrictModeEnabled();

    abstract RaftGroupId getGroupId(ISemaphore semaphore);

    abstract long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId);

    abstract long getThreadId(HazelcastInstance semaphoreInstance, RaftGroupId groupId);

    @Test
    public void testRetriedAcquireDoesNotCancelPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid, 1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid, 1, -1));

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        }, 10);
    }

    @Test(timeout = 30000)
    public void testNewAcquireCancelsPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid2, 1, -1));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testNewAcquireCancelsPendingAcquireRequestWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid2, 1, -1));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testTryAcquireWithTimeoutCancelsPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid2, 1, 100));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testNewTryAcquireWithTimeoutCancelsPendingAcquireRequestWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid2, 1, 100));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testNewTryAcquireWithoutTimeoutCancelsPendingAcquireRequestWhenAlreadyAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid2, 1, 0));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testNewTryAcquireWithoutTimeoutCancelsPendingAcquireRequestsWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        UUID invUid2 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        invocationManager.invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid2, 1, 0));

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testReleaseCancelsPendingAcquireRequestWhenPermitsAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid, 1, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        try {
            semaphore.release();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test
    public void testReleaseCancelsPendingAcquireRequestWhenNoPermitsAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        try {
            semaphore.release();
        } catch (IllegalArgumentException ignored) {
        }

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testDrainCancelsPendingAcquireRequestWhenNotAcquired() throws InterruptedException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        semaphore.drainPermits();

        try {
            f.join();
            fail();
        } catch (WaitKeyCancelledException ignored) {
        }
    }

    @Test(timeout = 30000)
    public void testRetriedAcquireReceivesPermitsOnlyOnce() throws InterruptedException, ExecutionException {
        semaphore.init(1);
        semaphore.acquire();
        semaphore.release();
        // if the session-aware semaphore is used, we guarantee that there is a session id now...

        final RaftGroupId groupId = getGroupId(semaphore);
        long sessionId = getSessionId(semaphoreInstance, groupId);
        long threadId = getThreadId(semaphoreInstance, groupId);
        UUID invUid1 = newUnsecureUUID();
        RaftInvocationManager invocationManager = getRaftInvocationManager(semaphoreInstance);

        InternalCompletableFuture<Object> f1 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(1, registry.getWaitTimeouts().size());
            }
        });

        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    semaphore.tryAcquire(20, 5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(2, registry.getWaitTimeouts().size());
            }
        });

        InternalCompletableFuture<Object> f2 = invocationManager
                .invoke(groupId, new AcquirePermitsOp(name, sessionId, threadId, invUid1, 2, MINUTES.toMillis(5)));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftSemaphoreService service = getNodeEngineImpl(semaphoreInstance).getService(RaftSemaphoreService.SERVICE_NAME);
                SemaphoreRegistry registry = service.getRegistryOrNull(groupId);
                assertNotNull(registry);
                assertEquals(3, registry.getWaitTimeouts().size());
            }
        });

        spawn(new Runnable() {
            @Override
            public void run() {
                semaphore.increasePermits(3);
            }
        }).get();


        f1.join();
        f2.join();

        assertEquals(2, semaphore.availablePermits());
    }

}
