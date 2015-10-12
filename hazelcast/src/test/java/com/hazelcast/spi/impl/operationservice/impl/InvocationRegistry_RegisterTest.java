package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry.SEQUENCE_ALIGNMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvocationRegistry_RegisterTest extends HazelcastTestSupport {

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
        initialHighPriorityCallId = registry.highSequence.get();
    }

    private Invocation newInvocation(Operation op) {
        return new PartitionInvocation(operationService, op, 0, 0, 0, false);
    }

    @Test
    public void construction() {
        assertTrue(registry.highInvocations.isEmpty());
        assertEquals(initialHighPriorityCallId, registry.highSequence.get());

        assertEquals(CONCURRENCY_LEVEL * SEQUENCE_ALIGNMENT, registry.lowSequences.length());
        for (int k = 0; k < CONCURRENCY_LEVEL; k++) {
            long actual = registry.lowSequences.get(k * SEQUENCE_ALIGNMENT);
            assertEquals(k, actual);
        }
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

        assertEquals(initialHighPriorityCallId - 1, registry.highSequence.get());
        assertEquals(1, registry.highInvocations.size());
        assertSame(invocation, registry.highInvocations.get(initialHighPriorityCallId));
        assertEquals(invocation.op.getCallId(), initialHighPriorityCallId);
    }

    @Test
    public void whenLowPriority_andTargetSpecific() {
        Invocation invocation = newInvocation(new DummyOperation(-1));
        registry.register(invocation);

        // make sure nothing is done on the priority section
        assertEquals(initialHighPriorityCallId, registry.highSequence.get());
        assertTrue(registry.highInvocations.isEmpty());

        // check if the call id was set correctly and if we can find the invocation for the given callId.
        long callId = invocation.op.getCallId();
        assertTrue("call id should be larger than 0", callId > 0);
        assertSame(invocation, registry.get(callId));

        // one of the lowSequences must have increased; we don't know which one because a lowSequence is selected at random.
        // so we need to look through all sequences.
        boolean found = false;
        for (int sequenceIndex = 0; sequenceIndex < CONCURRENCY_LEVEL; sequenceIndex++) {
            long sequence = registry.lowSequences.get(sequenceIndex * SEQUENCE_ALIGNMENT);
            if (sequence != sequenceIndex) {
                if (sequence == sequenceIndex + CONCURRENCY_LEVEL) {
                    found = true;
                    break;
                } else {
                    fail("Sequence at index:" + sequenceIndex + " has incorrect value:" + sequence);
                }
            }
        }
        assertTrue("No incremented sequence was found", found);
    }

    @Test
    public void whenLowPriority_andPartitionSpecific() {
        int partitionId = 0;
        int offset = (partitionId % CONCURRENCY_LEVEL) * SEQUENCE_ALIGNMENT;
        long oldCallId = registry.lowSequences.get(offset);

        Invocation invocation = newInvocation(new DummyOperation(partitionId));
        registry.register(invocation);

        // make sure nothing is done on the priority section
        assertEquals(initialHighPriorityCallId, registry.highSequence.get());
        assertTrue(registry.highInvocations.isEmpty());

        // now we check if the invocation can be find on the right position
        long expectedCallId = oldCallId + CONCURRENCY_LEVEL;
        assertEquals(expectedCallId, registry.lowSequences.get(offset));
        Invocation actual = registry.lowInvocations.get((int) (expectedCallId % registry.lowSequences.length()));
        assertSame(invocation, actual);
        // and of course the call id needs to be set.
        assertEquals(expectedCallId, invocation.op.getCallId());
    }

    @Test
    public void whenLowPriority_andOverflow() {

    }

    @Test
    public void whenLowPriority_andSlotAlreadyOccupied() {

    }

    @Test(expected = HazelcastOverloadException.class)
    public void whenLowPriority_andNoSpace_thenEventuallyHazelcastOverloadException() {
        for (; ; ) {
            DummyOperation op = new DummyOperation(0);
            setCallTimeout(op, 60000);
            Invocation invocation = newInvocation(op);
            registry.register(invocation);
        }
    }

    @Test
    public void whenMany() {
        List<Invocation> invocations = new LinkedList<Invocation>();
        Random random = new Random();

        // first we generate a bunch of invocations.
        for (int k = 0; k < 10 * 1000; k++) {
            // one 1 ten we make a priority operation
            boolean priority = random.nextInt(10) == 0;
            // we subtract 1 to get a non partition specific one from time to time.
            int partitionId = random.nextInt(271) - 1;
            Invocation invocation;
            if (priority) {
                invocation = newInvocation(new DummyPriorityOperation(partitionId));
            } else {
                invocation = newInvocation(new DummyOperation(partitionId));
            }
            invocations.add(invocation);
            registry.register(invocation);
        }

        // now we verify that each invocation can be found.
        for (Invocation invocation : invocations) {
            Invocation found = registry.get(invocation.op.getCallId());
            assertSame(invocation, found);
        }
    }
}
