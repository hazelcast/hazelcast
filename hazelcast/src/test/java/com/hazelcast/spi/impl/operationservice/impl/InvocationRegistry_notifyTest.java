package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
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

import static com.hazelcast.instance.GroupProperties.PROP_BACKPRESSURE_ENABLED;
import static com.hazelcast.instance.GroupProperties.PROP_OPERATION_CALL_TIMEOUT_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationRegistry_notifyTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;
    private HazelcastInstance local;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(PROP_BACKPRESSURE_ENABLED, "false");
        config.setProperty(PROP_OPERATION_CALL_TIMEOUT_MILLIS, "20000");
        local = createHazelcastInstance(config);
        warmUpPartitions(local);
        nodeEngine = getNodeEngineImpl(local);

        operationService = (OperationServiceImpl) getOperationService(local);
        invocationRegistry = operationService.invocationsRegistry;
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
        invocationRegistry.notify(new NormalResponse(value, callId, 0, false), null);

        assertEquals(value, invocation.invocationFuture.getSafely());
        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void normalResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.deregister(invocation);

        invocationRegistry.notify(new NormalResponse("foo", callId, 0, false), null);

        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void normalResponse_whenBackupCompletesFirst() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();

        invocationRegistry.notify(new BackupResponse(callId, false), null);
        assertSame(invocation, invocationRegistry.get(callId));

        Object value = "foo";
        invocationRegistry.notify(new NormalResponse(value, callId, 1, false), null);
        assertNull(invocationRegistry.get(callId));

        assertEquals(value, invocation.invocationFuture.getSafely());
    }

    //todo: more permutations with different number of backups arriving in different orders.

    @Test
    public void normalResponse_whenBackupMissing_thenEventuallySuccess() throws Exception {
        Invocation invocation = newInvocation();

        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();

        String result = "foo";
        invocationRegistry.notify(new NormalResponse(result, callId, 1, false), null);

        assertEquals(result, invocation.invocationFuture.get(1, TimeUnit.MINUTES));
        assertNull(invocationRegistry.get(callId));
    }

    @Test
    @Ignore
    public void normalResponse_whenOnlyBackupInThenRetry(){

    }

    // ==================== backupResponse ======================

    @Test
    public void backupResponse_whenInvocationExist() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        Object value = "foo";
        invocationRegistry.notify(new NormalResponse(value, callId, 1, false), null);
        assertSame(invocation, invocationRegistry.get(callId));

        invocationRegistry.notify(new BackupResponse(callId, false), null);
        assertNull(invocationRegistry.get(callId));

        assertEquals(value, invocation.invocationFuture.getSafely());
    }

    @Test
    public void backupResponse_whenInvocationMissing_thenNothingBadHappens() {
        Invocation invocation = newInvocation();
        long callId = invocation.op.getCallId();
        invocationRegistry.register(invocation);
        invocationRegistry.deregister(invocation);

        invocationRegistry.notify(new BackupResponse(callId, false), null);
        assertNull(invocationRegistry.get(callId));
    }

    // ==================== errorResponse ======================

    @Test
    public void errorResponse_whenInvocationExists() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        invocationRegistry.notify(new ErrorResponse(new ExpectedRuntimeException(), callId, false),
                null);

        try {
            invocation.invocationFuture.getSafely();
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

        invocationRegistry.notify(new ErrorResponse(new ExpectedRuntimeException(), callId, false),
                null);

        assertNull(invocationRegistry.get(callId));
    }

    // ==================== timeoutResponse =====================

    @Test
    public void timeoutResponse() {
        Invocation invocation = newInvocation();
        invocationRegistry.register(invocation);

        long callId = invocation.op.getCallId();
        invocationRegistry.notify(new CallTimeoutResponse(callId, false), null);

        assertNull(invocation.invocationFuture.getSafely());
        assertNull(invocationRegistry.get(callId));
    }
}
