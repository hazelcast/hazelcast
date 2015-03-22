package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationRegistryTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNodeEngineImpl(hz);
        operationService = (OperationServiceImpl)getOperationService(hz);
        invocationRegistry = new InvocationRegistry(operationService, 10);
    }

    @Test
    public void testConstruction() {
        // we need to make sure we start at 1 (at least >0) because 0 is treated as an unregistered call.
        assertEquals(1, invocationRegistry.nextCallId());
    }

    @Test
    public void register() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocationRegistry.register(invocation);

        assertEquals(2, invocationRegistry.nextCallId());
        assertEquals(1, invocation.op.getCallId());
    }

    @Test
    public void deregister_whenAlreadyDeregistered_thenIgnored() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocationRegistry.register(invocation);
        long callId = op.getCallId();

        invocationRegistry.deregister(invocation);
        invocationRegistry.deregister(invocation);
        assertNull(invocationRegistry.get(callId));
    }

    @Test
    public void deregister_whenRegistered_thenRemoved() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocationRegistry.register(invocation);
        long callId = op.getCallId();

        invocationRegistry.deregister(invocation);
        assertNull(invocationRegistry.get(callId));
    }
}
