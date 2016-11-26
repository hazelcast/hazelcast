package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithBackpressure;
import com.hazelcast.spi.impl.operationservice.impl.Invocation.Context;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.spi.OperationAccessor.hasActiveInvocation;
import static com.hazelcast.spi.properties.GroupProperty.BACKPRESSURE_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InvocationRegistryTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private ILogger logger;

    @Before
    public void setup() {
        logger = Mockito.mock(ILogger.class);
        final int capacity = 2;
        invocationRegistry = new InvocationRegistry(logger, new CallIdSequenceWithBackpressure(capacity, 1000));
    }

    private Invocation newInvocation() {
        return newInvocation(new DummyBackupAwareOperation());
    }

    private Invocation newInvocation(Operation op) {
        Invocation.Context context = new Context(null, null, null, null, null,
                1000, invocationRegistry, null, "", logger, null, null, null, null, null, null, null, null);
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
    public void register_whenAlreadyRegistered_thenException() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = newInvocation(op);
        invocationRegistry.register(invocation);
        final long originalCallId = invocationRegistry.getLastCallId();
        for (int i = 0; i < 10; i++) {
            try {
                invocationRegistry.register(invocation);
                fail();
            } catch (IllegalStateException e) {
                // expected
            }
            assertSame(invocation, invocationRegistry.get(originalCallId));
            assertEquals(originalCallId, invocation.op.getCallId());
        }
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

        assertFalse(invocation.isActive());
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
