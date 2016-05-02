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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationRegistry_GetTest extends HazelcastTestSupport {

    private static final int CONCURRENCY_LEVEL = 16;

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private InvocationRegistry registry;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        operationService = getOperationServiceImpl(hz);
        nodeEngine = getNode(hz).getNodeEngine();
        ILogger logger = nodeEngine.getLogger(InvocationRegistry.class);
        registry = new InvocationRegistry(nodeEngine.getProperties(), logger, CONCURRENCY_LEVEL);
    }

    private Invocation newInvocation(Operation op) {
        return new PartitionInvocation(operationService, op, 0, 0, 0, false);
    }

    @Test
    public void whenHighPriority_andNotExisting() {
        Invocation invocation = newInvocation(new DummyPriorityOperation());
        registry.register(invocation);

        long callId = invocation.op.getCallId();
        registry.deregister(invocation);

        Invocation found = registry.get(callId);
        assertNull(found);
    }

    @Test
    public void whenHighPriority() {
        Invocation invocation = newInvocation(new DummyPriorityOperation());
        registry.register(invocation);

        long callId = invocation.op.getCallId();

        Invocation found = registry.get(callId);
        assertSame(invocation, found);
    }

    @Test
    public void whenLowPriority_andNotExisting() {
        Invocation invocation = newInvocation(new DummyOperation());
        registry.register(invocation);

        long callId = invocation.op.getCallId();
        registry.deregister(invocation);

        Invocation found = registry.get(callId);
        assertNull(found);
    }

    @Test
    public void whenLowPriority() {
        Invocation invocation = newInvocation(new DummyOperation());
        registry.register(invocation);

        long callId = invocation.op.getCallId();

        Invocation found = registry.get(callId);
        assertSame(invocation, found);
    }

    @Test
    public void whenLowPriority_andAnotherInvocationInTheSameSlot() {

    }
}
