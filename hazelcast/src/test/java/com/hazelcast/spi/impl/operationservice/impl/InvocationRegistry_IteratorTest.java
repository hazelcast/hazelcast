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

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationRegistry_IteratorTest extends HazelcastTestSupport {

    private static final int CONCURRENCY_LEVEL = 16;

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private InvocationRegistry registry;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNode(hz).getNodeEngine();
        operationService = getOperationServiceImpl(hz);
        ILogger logger = nodeEngine.getLogger(InvocationRegistry.class);
        registry = new InvocationRegistry(nodeEngine.getProperties(), logger, CONCURRENCY_LEVEL);
    }

    private Invocation newInvocation(Operation op) {
        return new PartitionInvocation(operationService, op,  0, 0, 0, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void whenRemove() {
        Iterator<Invocation> it = registry.iterator();
        it.remove();
    }

    @Test
    public void whenLowPriorityInvocations() {
        Set<Invocation> invocations = new HashSet<Invocation>();
        invocations.add(newInvocation(new DummyOperation()));
        invocations.add(newInvocation(new DummyOperation()));
        invocations.add(newInvocation(new DummyOperation()));

        for (Invocation invocation : invocations) {
            registry.register(invocation);
        }

        Set<Invocation> found = new HashSet<Invocation>();
        for (Invocation invocation : registry) {
            found.add(invocation);
        }

        assertEquals(invocations, found);
    }

    @Test
    public void whenHighPriorityInvocations() {
        Set<Invocation> invocations = new HashSet<Invocation>();
        invocations.add(newInvocation(new DummyPriorityOperation()));
        invocations.add(newInvocation(new DummyPriorityOperation()));
        invocations.add(newInvocation(new DummyPriorityOperation()));

        for (Invocation invocation : invocations) {
            registry.register(invocation);
        }

        Set<Invocation> found = new HashSet<Invocation>();
        for (Invocation invocation : registry) {
            found.add(invocation);
        }

        assertEquals(invocations, found);
    }

    @Test
    public void whenMixed() {
        Set<Invocation> invocations = new HashSet<Invocation>();
        invocations.add(newInvocation(new DummyPriorityOperation()));
        invocations.add(newInvocation(new DummyPriorityOperation()));
        invocations.add(newInvocation(new DummyPriorityOperation()));
        invocations.add(newInvocation(new DummyOperation()));
        invocations.add(newInvocation(new DummyOperation()));

        for (Invocation invocation : invocations) {
            registry.register(invocation);
        }

        Set<Invocation> found = new HashSet<Invocation>();
        for (Invocation invocation : registry) {
            found.add(invocation);
        }

        assertEquals(invocations, found);
    }

    @Test(expected = NoSuchElementException.class)
    public void whenNextAndNoItems() {
        Iterator<Invocation> it = registry.iterator();
        it.next();
    }
}
