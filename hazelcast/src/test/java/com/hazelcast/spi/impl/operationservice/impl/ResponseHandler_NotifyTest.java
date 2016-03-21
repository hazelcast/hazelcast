package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
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
public class ResponseHandler_NotifyTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;
    private HazelcastInstance local;
    private ResponseHandler responseHandler;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS.getName(), "20000");
        local = createHazelcastInstance(config);
        warmUpPartitions(local);
        nodeEngine = getNodeEngineImpl(local);

        operationService = getOperationServiceImpl(local);
        invocationRegistry = operationService.invocationRegistry;
        responseHandler = operationService.getResponseHandler();
    }

    private Invocation newInvocation() {
        return newInvocation(new DummyBackupAwareOperation(1));
    }

    private Invocation newInvocation(Operation op) {
        Invocation invocation = new PartitionInvocation(operationService, op , 0, 0, 0, false);
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
        responseHandler.notifyNormalResponse(callId, value, 0, null);

        assertEquals(value, invocation.future.join());
        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void normalResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.deregister(invocation);

        responseHandler.notifyNormalResponse(callId, "foo", 0, null);

        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void normalResponse_whenBackupCompletesFirst() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();

        responseHandler.notifyBackupComplete(callId);
        assertSame(invocation, invocationRegistry.get(callId));

        Object value = "foo";
        responseHandler.notifyNormalResponse(callId, value, 1, null);
        assertNull(invocationRegistry.get(callId));

        assertEquals(value, invocation.future.join());
    }

    //todo: more permutations with different number of backups arriving in different orders.

    @Test
    public void normalResponse_whenBackupMissing_thenEventuallySuccess() throws Exception {
        Invocation invocation = newInvocation();

        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();

        String result = "foo";
        responseHandler.notifyNormalResponse(callId, result, 1, null);

        assertEquals(result, invocation.future.get(1, TimeUnit.MINUTES));
        assertNull(invocationRegistry.get(callId));
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
        responseHandler.notifyNormalResponse(callId, value, 1, null);
        assertSame(invocation, invocationRegistry.get(callId));

        responseHandler.notifyBackupComplete(callId);
        assertNull(invocationRegistry.get(callId));

        assertEquals(value, invocation.future.join());
    }

    @Test
    public void backupResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        long callId = invocation.op.getCallId();
        invocationRegistry.register(invocation);
        invocationRegistry.deregister(invocation);

        responseHandler.notifyBackupComplete(callId);
        assertNull(invocationRegistry.get(callId));
    }

    // ==================== errorResponse ======================

    @Test
    public void errorResponse_whenInvocationExists() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        responseHandler.notifyErrorResponse(callId, new ExpectedRuntimeException(), null);

        try {
            invocation.future.join();
            fail();
        } catch (ExpectedRuntimeException expected) {
        }

        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void errorResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.deregister(invocation);

        responseHandler.notifyErrorResponse(callId, new ExpectedRuntimeException(), null);

        assertNull(invocationRegistry.get(callId));
    }

    // ==================== timeoutResponse =====================

    @Test
    public void timeoutResponse() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        responseHandler.notifyCallTimeout(callId, null);

        try {
            assertNull(invocation.future.join());
            fail();
        } catch (OperationTimeoutException expected) {
        }

        assertNull(invocationRegistry.get(callId));
    }
}
