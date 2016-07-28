package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InvocationRegistryTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private OperationServiceImpl operationService;
    private HazelcastInstance local;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(BACKPRESSURE_ENABLED.getName(), "false");
        local = createHazelcastInstance(config);
        warmUpPartitions(local);

        operationService = (OperationServiceImpl) getOperationService(local);
        invocationRegistry = operationService.invocationRegistry;
    }

    private Invocation newInvocation() {
        return newInvocation(new DummyBackupAwareOperation());
    }

    private Invocation newInvocation(Operation op) {
        Invocation.Context context = operationService.invocationContext;
        return new PartitionInvocation(context, op, 0, 0, 0, false);
    }

    // ====================== register ===============================

    @Test
    public void register_Invocation() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = newInvocation(op);
        long oldCallId = invocationRegistry.getLastCallId();

        invocationRegistry.register(invocation);

        assertEquals(oldCallId + 1, op.getCallId());
        assertSame(invocation, invocationRegistry.get(op.getCallId()));
    }

    @Test
    @RequireAssertEnabled
    public void register_whenAlreadyRegistered_thenAssertionError() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = newInvocation(op);
        invocationRegistry.register(invocation);
        long oldCallId = invocationRegistry.getLastCallId();

        try {
            invocationRegistry.register(invocation);
            fail();
        } catch (AssertionError expected) {

        }

        assertSame(invocation, invocationRegistry.get(oldCallId));
        assertEquals(oldCallId, invocationRegistry.getLastCallId());
        assertEquals(oldCallId, invocation.op.getCallId());
    }

    // ====================== deregister ===============================

    @Test
    public void deregister_whenAlreadyDeregistered_thenIgnored() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = newInvocation(op);
        invocationRegistry.register(invocation);
        long callId = op.getCallId();

        invocationRegistry.deregister(invocation);
        invocationRegistry.deregister(invocation);
        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void deregister_whenSkipped() {
        Operation op = new DummyOperation();
        Invocation invocation = newInvocation(op);
        invocation.remote = false;

        invocationRegistry.register(invocation);
        invocationRegistry.deregister(invocation);

        assertEquals(0, invocation.op.getCallId());
    }

    @Test
    public void deregister_whenRegistered_thenRemoved() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = newInvocation(op);
        invocationRegistry.register(invocation);
        long callId = op.getCallId();

        invocationRegistry.deregister(invocation);
        assertNull(invocationRegistry.get(callId));
    }

    // ====================== size ===============================

    @Test
    public void test_size() {
        assertEquals(0, invocationRegistry.size());

        Invocation firstInvocation = newInvocation();
        invocationRegistry.register(firstInvocation);

        assertEquals(1, invocationRegistry.size());

        Invocation secondInvocation = newInvocation();
        invocationRegistry.register(secondInvocation);

        assertEquals(2, invocationRegistry.size());
    }

    // ===================== onMemberLeft ============================

    // ===================== reset ============================

    @Test
    public void reset_thenAllInvocationsMemberLeftException() throws ExecutionException, InterruptedException {
        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        invocationRegistry.register(invocation);
        long callId = invocation.op.getCallId();

        invocationRegistry.reset();

        InvocationFuture f = invocation.future;
        try {
            f.get();
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
            f.join();
            fail();
        } catch (HazelcastInstanceNotActiveException expected) {
        }

        assertNull(invocationRegistry.get(callId));
    }
}
