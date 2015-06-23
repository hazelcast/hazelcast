package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithBackpressure;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CallIdSequenceWithoutBackpressureTest extends HazelcastTestSupport {

    private static boolean LOCAL = false;
    private static boolean REMOTE = true;
    private static boolean SKIPPED = true;
    private static boolean NOT_SKIPPED = false;

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNode(hz).nodeEngine;
    }

    @Test
    public void test() {
        CallIdSequence.CallIdSequenceWithoutBackpressure sequence = new CallIdSequence.CallIdSequenceWithoutBackpressure();
        assertEquals(0, sequence.getLastCallId());
        assertEquals(Integer.MAX_VALUE, sequence.getMaxConcurrentInvocations());
    }

    @Test
    public void nextRepeatedly() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);
        for (long k = 1; k < 100; k++) {
            Invocation invocation = newInvocation(new DummyBackupAwareOperation());
            assertEquals(k, sequence.next(invocation));
        }
    }

    @Test
    public void next_whenNot_0() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);
        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        setCallId(invocation.op, 10);

        long lastCallId = sequence.getLastCallId();
        try {
            sequence.next(invocation);
            fail();
        } catch (AssertionError e) {
        }

        assertEquals(lastCallId, sequence.getLastCallId());
    }

    @Test
    public void next() {
        // regular operation
        next(new DummyOperation(), REMOTE, NOT_SKIPPED);
        next(new DummyOperation(), LOCAL, SKIPPED);

        // backup-aware operation
        next(new DummyBackupAwareOperation(), LOCAL, NOT_SKIPPED);
        next(new DummyBackupAwareOperation(), REMOTE, NOT_SKIPPED);

        //urgent operation
        next(new DummyPriorityOperation(), LOCAL, SKIPPED);
        next(new DummyPriorityOperation(), REMOTE, NOT_SKIPPED);
    }

    public void next(Operation operation, boolean remote, boolean skipped) {
        CallIdSequence.CallIdSequenceWithoutBackpressure sequence = new CallIdSequence.CallIdSequenceWithoutBackpressure();

        Invocation invocation = newInvocation(operation);
        invocation.remote = remote;
        long oldSequence = sequence.getLastCallId();

        long result = sequence.next(invocation);

        if (skipped) {
            assertEquals(Operation.CALL_ID_LOCAL_SKIPPED, result);
            assertEquals(oldSequence, sequence.getLastCallId());
        } else {
            assertEquals(oldSequence + 1, result);
            assertEquals(oldSequence + 1, sequence.getLastCallId());
        }
    }

    @Test
    public void complete() {
        CallIdSequence.CallIdSequenceWithoutBackpressure sequence = new CallIdSequence.CallIdSequenceWithoutBackpressure();
        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        long id = sequence.next(invocation);
        setCallId(invocation.op, id);

        long oldSequence = sequence.getLastCallId();

        sequence.complete(invocation);
        assertEquals(oldSequence, sequence.getLastCallId());
    }

    private Invocation newInvocation(Operation op) {
        return new PartitionInvocation(nodeEngine, null, op, 0, 0, 0, 0, 0, null, null, false);
    }
}
