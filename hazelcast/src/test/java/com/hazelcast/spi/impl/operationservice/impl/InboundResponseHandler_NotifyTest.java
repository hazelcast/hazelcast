package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InboundResponseHandler_NotifyTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private OperationServiceImpl operationService;
    private HazelcastInstance local;
    private InboundResponseHandler inboundResponseHandler;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "20000");
        local = createHazelcastInstance(config);
        warmUpPartitions(local);

        operationService = getOperationServiceImpl(local);
        invocationRegistry = operationService.invocationRegistry;
        inboundResponseHandler = operationService.getInboundResponseHandler();
    }

    private Invocation newInvocation() {
        return newInvocation(new DummyBackupAwareOperation(1));
    }

    private Invocation newInvocation(Operation op) {
        Invocation.Context context = operationService.invocationContext;
        Invocation invocation = new PartitionInvocation(context, op , 0, 0, 0, false);
        invocation.invTarget = getAddress(local);
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
            invocation.future.join();
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
            assertNull(invocation.future.join());
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
