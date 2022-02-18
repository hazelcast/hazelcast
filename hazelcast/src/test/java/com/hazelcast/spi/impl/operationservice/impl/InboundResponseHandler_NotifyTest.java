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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InboundResponseHandler_NotifyTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private OperationServiceImpl operationService;
    private InboundResponseHandler inboundResponseHandler;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "20000");
        HazelcastInstance local = createHazelcastInstance(config);
        warmUpPartitions(local);

        operationService = getOperationService(local);
        invocationRegistry = operationService.invocationRegistry;
        inboundResponseHandler = operationService.getBackupHandler();
    }

    private Invocation newInvocation() {
        return newInvocation(new DummyBackupAwareOperation(1));
    }

    private Invocation newInvocation(Operation op) {
        Invocation.Context context = operationService.invocationContext;
        Invocation invocation = new PartitionInvocation(context, op, 0, 0, 0, false, false);
        try {
            invocation.initInvocationTarget();
        } catch (Exception e) {
            fail(e.toString());
        }
        return invocation;
    }

    // ================== normalResponse ========================

    @Test
    public void normalResponse_whenInvocationExist() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        Object value = "foo";
        inboundResponseHandler.notifyNormalResponse(callId, value, 0, null);

        assertEquals(value, invocation.future.join());
        assertInvocationDeregisteredEventually(callId);
    }

    @Test
    public void normalResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.deregister(invocation);

        inboundResponseHandler.notifyNormalResponse(callId, "foo", 0, null);

        assertInvocationDeregisteredEventually(callId);
    }

    @Test
    public void normalResponse_whenBackupCompletesFirst() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();

        inboundResponseHandler.notifyBackupComplete(callId);
        assertSame(invocation, invocationRegistry.get(callId));

        Object value = "foo";
        inboundResponseHandler.notifyNormalResponse(callId, value, 1, null);
        assertInvocationDeregisteredEventually(callId);

        assertEquals(value, invocation.future.join());
    }

    //todo: more permutations with different number of backups arriving in different orders.

    @Test
    public void normalResponse_whenBackupMissing_thenEventuallySuccess() throws Exception {
        Invocation invocation = newInvocation();

        invocationRegistry.register(invocation);
        final long callId = invocation.op.getCallId();

        String result = "foo";
        inboundResponseHandler.notifyNormalResponse(callId, result, 1, null);

        assertEquals(result, invocation.future.get(1, TimeUnit.MINUTES));
        assertInvocationDeregisteredEventually(callId);
    }

    @Test
    @Ignore
    public void normalResponse_whenOnlyBackupInThenRetry() {
    }

    // ==================== backupResponse ======================

    @Test
    public void backupResponse_whenInvocationExist() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        Object value = "foo";
        inboundResponseHandler.notifyNormalResponse(callId, value, 1, null);
        assertSame(invocation, invocationRegistry.get(callId));

        inboundResponseHandler.notifyBackupComplete(callId);
        assertInvocationDeregisteredEventually(callId);

        assertEquals(value, invocation.future.join());
    }

    @Test
    public void backupResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        long callId = invocation.op.getCallId();
        invocationRegistry.register(invocation);
        invocationRegistry.deregister(invocation);

        inboundResponseHandler.notifyBackupComplete(callId);
        assertInvocationDeregisteredEventually(callId);
    }

    // ==================== errorResponse ======================

    @Test
    public void errorResponse_whenInvocationExists() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        inboundResponseHandler.notifyErrorResponse(callId, new ExpectedRuntimeException(), null);

        try {
            invocation.future.joinInternal();
            fail();
        } catch (ExpectedRuntimeException expected) {
        }

        assertInvocationDeregisteredEventually(callId);
    }

    @Test
    public void errorResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.deregister(invocation);

        inboundResponseHandler.notifyErrorResponse(callId, new ExpectedRuntimeException(), null);

        assertInvocationDeregisteredEventually(callId);
    }

    // ==================== timeoutResponse =====================

    @Test
    public void timeoutResponse() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        inboundResponseHandler.notifyCallTimeout(callId, null);

        try {
            assertNull(invocation.future.joinInternal());
            fail();
        } catch (OperationTimeoutException expected) {
        }

        assertInvocationDeregisteredEventually(callId);
    }

    private void assertInvocationDeregisteredEventually(final long callId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(invocationRegistry.get(callId));
            }
        });
    }
}
