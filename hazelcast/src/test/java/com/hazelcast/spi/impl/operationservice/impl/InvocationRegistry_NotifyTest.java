package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
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

import static com.hazelcast.instance.GroupProperty.BACKPRESSURE_ENABLED;
import static com.hazelcast.instance.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationRegistry_NotifyTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;
    private HazelcastInstance local;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED, "false");
        config.setProperty(OPERATION_CALL_TIMEOUT_MILLIS, "20000");
        local = createHazelcastInstance(config);
        warmUpPartitions(local);
        nodeEngine = getNodeEngineImpl(local);

        operationService = (OperationServiceImpl) getOperationService(local);
        invocationRegistry = operationService.invocationRegistry;
    }

    private Invocation newInvocation() {
        return newInvocation(new DummyBackupAwareOperation(1));
    }

    private Invocation newInvocation(Operation op) {
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, op.getPartitionId(), 0, 0, 0, 0, null, false);
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
        invocationRegistry.notifyNormalResponse(callId, value, 0, null);

        assertEquals(value, invocation.future.getSafely());
        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void normalResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.deregister(invocation);

        invocationRegistry.notifyNormalResponse(callId, "foo", 0, null);

        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void normalResponse_whenBackupCompletesFirst() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();

        invocationRegistry.notifyBackupComplete(callId);
        assertSame(invocation, invocationRegistry.get(callId));

        Object value = "foo";
        invocationRegistry.notifyNormalResponse(callId, value, 1, null);
        assertNull(invocationRegistry.get(callId));

        assertEquals(value, invocation.future.getSafely());
    }

    //todo: more permutations with different number of backups arriving in different orders.

    @Test
    public void normalResponse_whenBackupMissing_thenEventuallySuccess() throws Exception {
        Invocation invocation = newInvocation();

        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();

        String result = "foo";
        invocationRegistry.notifyNormalResponse(callId, result, 1, null);

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
        invocationRegistry.notifyNormalResponse(callId, value, 1, null);
        assertSame(invocation, invocationRegistry.get(callId));

        invocationRegistry.notifyBackupComplete(callId);
        assertNull(invocationRegistry.get(callId));

        assertEquals(value, invocation.future.getSafely());
    }

    @Test
    public void backupResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        long callId = invocation.op.getCallId();
        invocationRegistry.register(invocation);
        invocationRegistry.deregister(invocation);

        invocationRegistry.notifyBackupComplete(callId);
        assertNull(invocationRegistry.get(callId));
    }

    // ==================== errorResponse ======================

    @Test
    public void errorResponse_whenInvocationExists() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        invocationRegistry.notifyErrorResponse(callId, new ExpectedRuntimeException(), null);

        try {
            invocation.future.getSafely();
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

        invocationRegistry.notifyErrorResponse(callId, new ExpectedRuntimeException(), null);

        assertNull(invocationRegistry.get(callId));
    }

    // ==================== timeoutResponse =====================

    @Test
    public void timeoutResponse() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        invocationRegistry.notifyCallTimeout(callId, null);

        assertNull(invocation.future.getSafely());
        assertNull(invocationRegistry.get(callId));
    }
}
