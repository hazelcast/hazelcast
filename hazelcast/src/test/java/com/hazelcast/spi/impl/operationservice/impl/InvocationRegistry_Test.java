package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InvocationRegistry_Test extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;
    private HazelcastInstance local;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        local = createHazelcastInstance(config);
        warmUpPartitions(local);
        nodeEngine = getNodeEngineImpl(local);

        operationService = (OperationServiceImpl) getOperationService(local);
        invocationRegistry = operationService.invocationRegistry;
    }

    private Invocation newInvocation(Operation op) {
        return new PartitionInvocation(operationService, op, 0, 0, 0, false);
    }

    // ====================== size ===============================

    @Test
    public void size_whenEmpty() {
        assertEquals(0, invocationRegistry.size());
    }

    @Test
    public void size_whenLowPriorityOperations() {
        invocationRegistry.register(newInvocation(new DummyOperation(1)));
        invocationRegistry.register(newInvocation(new DummyOperation(1)));

        int actual = invocationRegistry.size();

        assertEquals(2, actual);
    }

    @Test
    public void size_whenHighPriorityOperations() {
        invocationRegistry.register(newInvocation(new DummyPriorityOperation(1)));
        invocationRegistry.register(newInvocation(new DummyPriorityOperation(1)));

        int actual = invocationRegistry.size();

        assertEquals(2, actual);
    }

    @Test
    public void size_whenMixed() {
        invocationRegistry.register(newInvocation(new DummyOperation(1)));
        invocationRegistry.register(newInvocation(new DummyOperation(1)));
        invocationRegistry.register(newInvocation(new DummyPriorityOperation(1)));

        int actual = invocationRegistry.size();

        assertEquals(3, actual);
    }

    // ===================== reset ============================

    @Test
    public void reset_thenAllInvocationsMemberLeftException() throws ExecutionException, InterruptedException {
        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();

        invocationRegistry.reset();

        InvocationFuture f = invocation.future;
        try {
            f.get();//todo: strange to get a memberleftexception and not wrapped in ExecutionException.
            fail();
        } catch (MemberLeftException expected) {
        }

        assertNull(invocationRegistry.get(callId));
    }

    // ===================== shutdown ============================

    @Test
    public void shutdown_thenAllInvocationsAborted() throws ExecutionException, InterruptedException {
        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();
        invocationRegistry.shutdown();

        InvocationFuture f = invocation.future;
        try {
            f.getSafely();
            fail();
        } catch (HazelcastInstanceNotActiveException expected) {
        }

        assertNull(invocationRegistry.get(callId));
    }
}
