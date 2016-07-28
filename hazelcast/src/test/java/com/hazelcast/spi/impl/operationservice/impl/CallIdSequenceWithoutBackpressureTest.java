package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithBackpressure;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CallIdSequenceWithoutBackpressureTest extends HazelcastTestSupport {

    private static boolean LOCAL = false;
    private static boolean REMOTE = true;

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNode(hz).nodeEngine;
        operationService = getOperationServiceImpl(hz);
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
    @RequireAssertEnabled
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
        next(new DummyOperation(), REMOTE);
        next(new DummyOperation(), LOCAL);

        // backup-aware operation
        next(new DummyBackupAwareOperation(), LOCAL);
        next(new DummyBackupAwareOperation(), REMOTE);

        //urgent operation
        next(new DummyPriorityOperation(), LOCAL);
        next(new DummyPriorityOperation(), REMOTE);
    }

    public void next(Operation operation, boolean remote) {
        CallIdSequence.CallIdSequenceWithoutBackpressure sequence = new CallIdSequence.CallIdSequenceWithoutBackpressure();

        Invocation invocation = newInvocation(operation);
        invocation.remote = remote;
        long oldSequence = sequence.getLastCallId();

        long result = sequence.next(invocation);

        assertEquals(oldSequence + 1, result);
        assertEquals(oldSequence + 1, sequence.getLastCallId());
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
        Invocation.Context context = new Invocation.Context(null, null, null, null, null, 0, null, null, null, null, null, null,
                null, null, null, null, null, null);
        return new PartitionInvocation(context, op, 0, 0, 0, false);
    }
}
