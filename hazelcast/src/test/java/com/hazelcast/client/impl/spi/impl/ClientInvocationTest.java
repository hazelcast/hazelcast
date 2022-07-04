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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapSizeCodec;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientInvocationTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }


    /**
     * When a async operation fails because of a node termination,
     * failure stack trace is copied incrementally for each async invocation/future
     * <p/>
     * see https://github.com/hazelcast/hazelcast/issues/4192
     */
    @Test
    public void executionCallback_TooLongThrowableStackTrace() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());

        DummyEntryProcessor ep = new DummyEntryProcessor();

        int count = 100;
        FailureExecutionCallback[] callbacks = new FailureExecutionCallback[count];
        String key = randomString();
        for (int i = 0; i < count; i++) {
            callbacks[i] = new FailureExecutionCallback();
            map.submitToKey(key, ep).whenCompleteAsync(callbacks[i]);
        }

        // crash the server
        getNode(server).getServer().shutdown();
        server.getLifecycleService().terminate();

        int callBackCount = 0;
        for (FailureExecutionCallback callback : callbacks) {
            callBackCount++;
            assertOpenEventually("Callback should be notified on time! callbackCount:" + callBackCount, callback.latch);

            Throwable failure = callback.failure;
            if (failure == null) {
                continue;
            }
            int stackTraceLength = failure.getStackTrace().length;
            assertTrue("Failure stack trace should not be too long! Current: "
                    + stackTraceLength, stackTraceLength < 50);

            Throwable cause = failure.getCause();
            if (cause == null) {
                continue;
            }
            stackTraceLength = cause.getStackTrace().length;
            assertTrue("Cause stack trace should not be too long! Current: "
                    + stackTraceLength, stackTraceLength < 50);
        }
    }

    @Test
    public void executionCallback_FailOnShutdown() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig().setClusterConnectTimeoutMillis(10000);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);

        IMap<Object, Object> map = client.getMap(randomName());
        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnectedLatch.countDown();
            }
        });
        server.shutdown();
        assertOpenEventually(disconnectedLatch);
        int n = 100;
        final CountDownLatch errorLatch = new CountDownLatch(n);
        CompletableFuture[] userCallbackFutures = new CompletableFuture[n];
        for (int i = 0; i < n; i++) {
            // submitStage is completed with HazelcastClientNotActiveException
            CompletionStage<Object> submitStage = map.submitToKey(randomString(), new DummyEntryProcessor());
            // a user-supplied callback submitted to default executor will not be executed with RejectedExecutionException
            CompletableFuture<Object> userCallbackFuture = submitStage.whenCompleteAsync((v, t) -> {
                fail("This must not be executed");
            }).toCompletableFuture();
            userCallbackFutures[i] = userCallbackFuture;
            userCallbackFuture.whenCompleteAsync((v, t) -> {
                if (t instanceof HazelcastClientNotActiveException) {
                    errorLatch.countDown();
                } else {
                    throw rethrow(t);
                }
            });
        }
        FutureUtil.waitWithDeadline(Arrays.asList(userCallbackFutures), 30, TimeUnit.SECONDS, t -> {
            // t is ExecutionException < HazelcastClientNotActiveException
            if (t.getCause() == null
                    || !(t.getCause() instanceof HazelcastClientNotActiveException)) {
                System.out.println("Throwable was unexpected instance of "
                        + (t.getCause() == null ? t.getClass() : t.getCause().getClass()));
                throw rethrow(t);
            }
        });
        assertOpenEventually("Not all of the requests failed", errorLatch);
    }

    private static class DummyEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            LockSupport.parkNanos(10000);
            return null;
        }

        @Override
        public EntryProcessor getBackupProcessor() {
            return null;
        }
    }


    private static class FailureExecutionCallback implements BiConsumer<Object, Throwable> {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile Throwable failure;

        @Override
        public void accept(Object o, Throwable throwable) {
            if (throwable != null) {
                failure = throwable;
            }
            latch.countDown();
        }
    }

    @Test
    public void invokeOnPartitionOwnerRedirectsToRandom_WhenPartitionOwnerIsnull() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        int ownerlessPartition = 4000;
        ClientInvocation invocation = new ClientInvocation(client, request, "map", ownerlessPartition);
        assertEquals(0, MapSizeCodec.decodeResponse(invocation.invoke().get()));
    }

    @Test
    public void invokeOnMemberRedirectsToRandom_whenMemberIsNotInMemberList() throws Exception {
        hazelcastFactory.newHazelcastInstance();
        UUID unavailableTarget = UUID.randomUUID();
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        ClientInvocation invocation = new ClientInvocation(client, request, "map", unavailableTarget);
        assertEquals(0, MapSizeCodec.decodeResponse(invocation.invoke().get()));
    }

    @Test(expected = OperationTimeoutException.class)
    public void invokeOnPartitionOwner_redirectDisallowedToRandom_WhenPartitionOwnerIsnull() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "1");
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient(config));

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        int ownerlessPartition = 4000;
        ClientInvocation invocation = new ClientInvocation(client, request, "map", ownerlessPartition);
        invocation.disallowRetryOnRandom();
        invocation.invoke().joinInternal();
    }

    @Test(expected = OperationTimeoutException.class)
    public void invokeOnMember_redirectDisallowedToRandom_whenMemberIsNotInMemberList() {
        hazelcastFactory.newHazelcastInstance();
        UUID unavailableTarget = UUID.randomUUID();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "1");
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient(config));

        ClientMessage request = MapSizeCodec.encodeRequest("test");
        ClientInvocation invocation = new ClientInvocation(client, request, "map", unavailableTarget);
        invocation.disallowRetryOnRandom();
        invocation.invoke().joinInternal();
    }
}
