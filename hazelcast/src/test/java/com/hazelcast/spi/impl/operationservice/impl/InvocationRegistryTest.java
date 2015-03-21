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

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class InvocationRegistryTest extends HazelcastTestSupport {

    private InvocationRegistry invocationRegistry;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;
    private HazelcastInstance[] cluster;
    private HazelcastInstance local;
    private HazelcastInstance remote;

    @Before
    public void setup() {
        cluster = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(cluster);
        local = cluster[0];
        remote = cluster[1];
        nodeEngine = getNodeEngineImpl(local);
        operationService = (OperationServiceImpl) getOperationService(local);
        invocationRegistry = new InvocationRegistry(operationService, 10, TimeUnit.MINUTES.toMillis(1));
    }

    // ====================== register ===============================

    @Test
    public void register_whenLocal_and_notBackupAware_thenSkipped() {
        Operation op = new DummyOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocation.remote = false;

        long oldCallId = invocationRegistry.getLastCallId();

        invocationRegistry.register(invocation);

        assertEquals(oldCallId, invocationRegistry.getLastCallId());
        assertEquals(0, invocation.op.getCallId());
        assertNull(invocationRegistry.get(op.getCallId()));
    }

    @Test
    public void register_whenRemote_and_notBackupAware_thenRegistered() {
        Operation op = new DummyOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocation.remote = true;
        long oldCallId = invocationRegistry.getLastCallId();

        invocationRegistry.register(invocation);

        assertEquals(oldCallId + 1, invocationRegistry.getLastCallId());
        assertEquals(oldCallId + 1, op.getCallId());
        assertSame(invocation, invocationRegistry.get(op.getCallId()));
    }

    @Test
    public void register_whenBackupAware_thenRegistered() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocation.remote = false;
        long oldCallId = invocationRegistry.getLastCallId();

        invocationRegistry.register(invocation);

        assertEquals(oldCallId + 1, invocationRegistry.getLastCallId());
        assertEquals(oldCallId + 1, invocation.op.getCallId());
        assertSame(invocation, invocationRegistry.get(op.getCallId()));
    }

    @Test
    public void register_whenAlreadyRegistered_thenFirstUnregistered() {
        Operation op = new DummyBackupAwareOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);

        invocationRegistry.register(invocation);
        long oldCallId = invocationRegistry.getLastCallId();
        invocationRegistry.register(invocation);

        assertNull(invocationRegistry.get(oldCallId));
        assertEquals(oldCallId + 1, invocationRegistry.getLastCallId());
        assertEquals(oldCallId + 1, invocation.op.getCallId());
        assertSame(invocation, invocationRegistry.get(op.getCallId()));
    }

    // ====================== deregister ===============================

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
    public void deregister_whenSkipped() {
        Operation op = new DummyOperation();
        Invocation invocation = new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
        invocation.remote = false;

        invocationRegistry.register(invocation);
        invocationRegistry.deregister(invocation);

        assertEquals(0, invocation.op.getCallId());
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

    // ====================== size and isEmpty===============================

    @Test
    public void size_and_isEmpty() {
        assertEquals(0, invocationRegistry.size());
        assertTrue(invocationRegistry.isEmpty());

        Invocation firstInvocation = newInvocation();
        invocationRegistry.register(firstInvocation);

        assertEquals(1, invocationRegistry.size());
        assertFalse(invocationRegistry.isEmpty());

        Invocation secondInvocation = newInvocation();
        invocationRegistry.register(secondInvocation);

        assertEquals(2, invocationRegistry.size());
        assertFalse(invocationRegistry.isEmpty());
    }

    private Invocation newInvocation() {
        Operation op = new DummyBackupAwareOperation();
        return new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, false);
    }
}
