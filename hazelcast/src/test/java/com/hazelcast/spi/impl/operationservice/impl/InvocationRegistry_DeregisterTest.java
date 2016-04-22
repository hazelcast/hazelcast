package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationRegistry_DeregisterTest extends HazelcastTestSupport {

    private static final int CONCURRENCY_LEVEL = 16;

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private InvocationRegistry registry;
    private long initialHighPriorityCallId;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNode(hz).getNodeEngine();
        operationService = getOperationServiceImpl(hz);
        ILogger logger = nodeEngine.getLogger(InvocationRegistry.class);
        registry = new InvocationRegistry(nodeEngine.getProperties(), logger, CONCURRENCY_LEVEL);
        initialHighPriorityCallId = registry.prioritySequence.get();
    }

    @Test
    public void whenHighPriority_andNotRegistered() {
        Invocation invocation = newInvocation(new DummyPriorityOperation());
        setCallId(invocation.op, -100);

        registry.deregister(invocation);

        assertTrue(registry.priorityInvocations.isEmpty());
        assertEquals(0, invocation.op.getCallId());
    }

    @Test
    public void whenHighPriority_andPartitionSpecific() {
        whenHighPriority(0);
    }

    @Test
    public void whenHighPriority_andTargetSpecific() {
        whenHighPriority(-1);
    }

    public void whenHighPriority(int partitionId) {
        Invocation invocation = newInvocation(new DummyPriorityOperation(partitionId));
        registry.register(invocation);
        registry.deregister(invocation);

        assertEquals(initialHighPriorityCallId - 1, registry.prioritySequence.get());
        assertTrue(registry.priorityInvocations.isEmpty());
        assertNull(registry.priorityInvocations.get(-1l));
        assertEquals(0, invocation.op.getCallId());
    }

    @Test
    public void whenLowPriority_andPartitionSpecific() {
        whenLowPriority(1);
    }

    @Test
    public void whenLowPriority_andTargetSpecific() {
        whenLowPriority(-1);
    }

    public void whenLowPriority(int partitionId) {
        Invocation invocation = newInvocation(new DummyOperation(partitionId));
        registry.register(invocation);

        int invocationOffset = registry.regularInvocationIndex(invocation.op.getCallId());
        registry.deregister(invocation);

        assertNull(registry.regularInvocations.get(invocationOffset));
        // and of course the call id needs to be set.
        assertEquals(0, invocation.op.getCallId());
    }

    @Test
    public void whenLowPriority_andNotRegistered() {
        Invocation invocation = newInvocation(new DummyOperation());
        registry.register(invocation);
        registry.deregister(invocation);

        assertEquals(initialHighPriorityCallId, registry.prioritySequence.get());
        assertTrue(registry.priorityInvocations.isEmpty());
        assertNull(registry.priorityInvocations.get(-1l));
        assertEquals(0, invocation.op.getCallId());
    }

    @Test
    public void whenLowPriority_andNotRegistered_butAnotherInvocationInSameSlot() {
        // first we store an original invocation
        Invocation initialInvocation = newInvocation(new DummyOperation());
        registry.register(initialInvocation);

        // then we force overwriting this invocation by a newer one that falls into the same slot.
        Invocation newerInvocation = newInvocation(new DummyOperation());
        setCallId(newerInvocation.op, initialInvocation.op.getCallId() + registry.regularInvocationsLength);
        int index = registry.regularInvocationIndex(initialInvocation.op.getCallId());
        assertSame(initialInvocation, registry.regularInvocations.get(index));
        registry.regularInvocations.set(index, newerInvocation);

        // now we try to deregister the initial invocation.
        registry.deregister(initialInvocation);

        assertEquals(initialHighPriorityCallId, registry.prioritySequence.get());
        assertTrue(registry.priorityInvocations.isEmpty());
        assertSame(newerInvocation, registry.get(newerInvocation.op.getCallId()));
        assertEquals(0, initialInvocation.op.getCallId());
    }

    private Invocation newInvocation(Operation op) {
        return new PartitionInvocation(operationService, op, 0, 0, 0, false);
    }
}
