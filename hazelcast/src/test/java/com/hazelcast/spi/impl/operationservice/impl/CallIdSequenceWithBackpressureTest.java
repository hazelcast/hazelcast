package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastOverloadException;
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

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithBackpressure.MAX_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CallIdSequenceWithBackpressureTest extends HazelcastTestSupport {

    private static boolean LOCAL = false;
    private static boolean REMOTE = true;

    private HazelcastInstance hz;
    private NodeEngineImpl nodeEngine;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        nodeEngine = getNode(hz).nodeEngine;
    }

    @Test
    public void test() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);
        assertEquals(0, sequence.getLastCallId());
        assertEquals(100, sequence.getMaxConcurrentInvocations());
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
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);

        Invocation invocation = newInvocation(operation);
        invocation.remote = remote;
        long oldSequence = sequence.getLastCallId();

        long result = sequence.next(invocation);

        assertEquals(oldSequence + 1, result);

        // the sequence always needs to increase because we need to know how many invocations are done
        assertEquals(oldSequence + 1, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_thenBlockTillCapacity() throws InterruptedException {
        final CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(1, 60000);
        long oldLastCallId = sequence.getLastCallId();

        final CountDownLatch nextCalledLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                Invocation invocation = newInvocation(new DummyBackupAwareOperation());

                long callId = sequence.next(invocation);
                setCallId(invocation.op, callId);
                nextCalledLatch.countDown();

                sleepSeconds(3);

                sequence.complete(invocation);
            }
        });

        nextCalledLatch.await();

        Invocation invocation = newInvocation(new DummyBackupAwareOperation());

        long result = sequence.next(invocation);

        assertEquals(oldLastCallId + 2, result);
        assertEquals(oldLastCallId + 2, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_thenBlockTillTimeout() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(1, 2000);

        // first invocation consumes the invocation
        Invocation firstInvocation = newInvocation(new DummyBackupAwareOperation());
        sequence.next(firstInvocation);

        long oldLastCallId = sequence.getLastCallId();

        Invocation secondInvocation = newInvocation(new DummyBackupAwareOperation());
        try {
            sequence.next(secondInvocation);
            fail();
        } catch (HazelcastOverloadException expected) {

        }

        assertEquals(oldLastCallId, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_andPriorityItem_thenNoBackPressure() {
        final CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(1, 60000);

        //first we consume all invocations
        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        sequence.next(invocation);

        long oldLastCallId = sequence.getLastCallId();

        Invocation priorityInvocation = newInvocation(new DummyPriorityOperation());
        long result = sequence.next(priorityInvocation);
        assertEquals(oldLastCallId + 1, result);
        assertEquals(oldLastCallId + 1, sequence.getLastCallId());
    }

    @Test
    public void complete() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);

        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        long callId = sequence.next(invocation);
        setCallId(invocation.op, callId);

        long oldSequence = sequence.getLastCallId();
        long oldTail = sequence.getTail();
        sequence.complete(invocation);

        assertEquals(oldSequence, sequence.getLastCallId());
        assertEquals(oldTail + 1, sequence.getTail());
    }

    @Test
    public void completeLocalCall() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);

        Invocation invocation = newInvocation(new DummyOperation());
        setCallId(invocation.op, 0);

        long oldSequence = sequence.getLastCallId();
        long oldTail = sequence.getTail();
        sequence.complete(invocation);

        assertEquals(oldSequence, sequence.getLastCallId());
        assertEquals(oldTail, sequence.getTail());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void complete_whenNoMatchingNext() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);

        Invocation invocation = newInvocation(new DummyBackupAwareOperation());
        setCallId(invocation.op, sequence.next(invocation));

        sequence.complete(invocation);

        sequence.complete(invocation);
    }

    @Test
    public void sleep_whenInterrupted() {
        Thread.currentThread().interrupt();
        assertTrue(CallIdSequence.CallIdSequenceWithBackpressure.sleep(10));
    }

    @Test
    public void sleep_whenNotInterrupted() {
        Thread.interrupted();//clears the flag
        assertFalse(CallIdSequence.CallIdSequenceWithBackpressure.sleep(10));
    }

    @Test
    public void nextDelay() {
        assertEquals(MAX_DELAY_MS, CallIdSequenceWithBackpressure.nextDelay(10000, MAX_DELAY_MS / 2));
        assertEquals(MAX_DELAY_MS, CallIdSequenceWithBackpressure.nextDelay(10000, MAX_DELAY_MS));
        assertEquals(10, CallIdSequenceWithBackpressure.nextDelay(10, MAX_DELAY_MS));
    }

    private Invocation newInvocation(Operation op) {
        Invocation.Context context = new Invocation.Context(null, null, null, null, null, 0, null, null, null, null, null, null,
                null, null, null, null, null, null);
        return new PartitionInvocation(context, op, 0, 0, 0, false);
    }
}
