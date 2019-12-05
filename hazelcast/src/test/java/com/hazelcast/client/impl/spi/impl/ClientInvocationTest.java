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

package com.hazelcast.client.impl.spi.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Matchers;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.util.ThreadUtil.getThreadId;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

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
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());

        DummyEntryProcessor ep = new DummyEntryProcessor();

        int count = 100;
        FailureExecutionCallback[] callbacks = new FailureExecutionCallback[count];
        String key = randomString();
        for (int i = 0; i < count; i++) {
            callbacks[i] = new FailureExecutionCallback();
            map.submitToKey(key, ep, callbacks[i]);
        }

        // crash the server
        getNode(server).getNetworkingService().shutdown();
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

    // This test is now failing. Reason is that failure to schedule an execution callback
    // resulted in the callback's `onFailure` method being called, with the `RejectedExecutionException`
    // as argument. This seems wrong but maybe some behaviour/functionality (especially related to shutdown)
    // relies on this and becomes broken in 4.0.
    // See also local scratch_74 for behaviour of CompletableFuture when callback cannot be submitted for execution
    @Ignore
    @Test
    public void executionCallback_FailOnShutdown() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

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
        for (int i = 0; i < n; i++) {
            try {
                map.submitToKey(randomString(), new DummyEntryProcessor(), new ExecutionCallback() {
                    @Override
                    public void onResponse(Object response) {
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        errorLatch.countDown();
                    }
                });
            } catch (Exception e) {
                errorLatch.countDown();
            }
        }
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


    private static class FailureExecutionCallback implements ExecutionCallback {
        final CountDownLatch latch = new CountDownLatch(1);
        volatile Throwable failure;

        @Override
        public void onResponse(Object response) {
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            failure = t;
            latch.countDown();
        }
    }

    @Test(expected = TargetNotMemberException.class)
    public void invokeOnPartitionOwnerWhenPartitionTableNotUpdated() throws IOException, ExecutionException, InterruptedException {
        hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        HazelcastClientInstanceImpl clientInstanceImpl = getHazelcastClientInstanceImpl(client);
        SmartClientInvocationService spyInvocationService
                = spy((SmartClientInvocationService) clientInstanceImpl.getInvocationService());

        //trying to simulate late partition table update here in case a member removed
        //in that case, the member that partitionService returns should not be in member list
        //we are simulating that by returning false when a membership of a member is asked
        when(spyInvocationService.isMember(Matchers.<Address>any())).thenReturn(false);

        SerializationService serializationService = clientInstanceImpl.getSerializationService();
        ClientMessage request = MapGetCodec.encodeRequest("test", serializationService.toData("test"), getThreadId());
        ClientInvocation invocation = new ClientInvocation(clientInstanceImpl, request, "map", 1);
        spyInvocationService.invokeOnPartitionOwner(invocation, 1);
    }
}
