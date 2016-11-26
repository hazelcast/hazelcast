package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithBackpressure;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.OperationAccessor.setCallId;
import static com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithBackpressure.MAX_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CallIdSequenceWithBackpressureTest extends HazelcastTestSupport {

    CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000);

    @Test
    public void test() {
        assertEquals(0, sequence.getLastCallId());
        assertEquals(100, sequence.getMaxConcurrentInvocations());
    }

    @Test
    public void whenNext_thenSequenceIncrements() {
        // regular operation
        testNext(new DummyOperation());

        // backup-aware operation
        testNext(new DummyBackupAwareOperation());

        // urgent operation
        testNext(new DummyPriorityOperation());
    }

    private void testNext(Operation operation) {
        long oldSequence = sequence.getLastCallId();
        long result = nextCallId(sequence, operation.isUrgent());
        assertEquals(oldSequence + 1, result);
        assertEquals(oldSequence + 1, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_thenBlockTillCapacity() throws InterruptedException {
        sequence = new CallIdSequenceWithBackpressure(1, 60000);
        final long oldLastCallId = sequence.getLastCallId();

        final CountDownLatch nextCalledLatch = new CountDownLatch(1);
        spawn(new Runnable() {
            @Override
            public void run() {
                DummyBackupAwareOperation op = new DummyBackupAwareOperation();
                long callId = nextCallId(sequence, op.isUrgent());
                setCallId(op, callId);
                nextCalledLatch.countDown();
                sleepSeconds(3);
                sequence.complete();
            }
        });

        nextCalledLatch.await();

        long result = nextCallId(sequence, false);
        assertEquals(oldLastCallId + 2, result);
        assertEquals(oldLastCallId + 2, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_thenBlockTillTimeout() {
        sequence = new CallIdSequenceWithBackpressure(1, 2000);

        // first invocation consumes the available call ID
        nextCallId(sequence, false);

        long oldLastCallId = sequence.getLastCallId();
        try {
            sequence.next(false);
            fail();
        } catch (TimeoutException e) {
            // expected
        }

        assertEquals(oldLastCallId, sequence.getLastCallId());
    }

    @Test
    public void when_overCapacityButPriorityItem_then_noBackpressure() {
        final CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(1, 60000);

        // occupy the single call ID slot
        nextCallId(sequence, true);

        long oldLastCallId = sequence.getLastCallId();

        long result = nextCallId(sequence, true);
        assertEquals(oldLastCallId + 1, result);
        assertEquals(oldLastCallId + 1, sequence.getLastCallId());
    }

    @Test
    public void whenComplete_thenTailIncrements() {
        nextCallId(sequence, false);

        long oldSequence = sequence.getLastCallId();
        long oldTail = sequence.getTail();
        sequence.complete();

        assertEquals(oldSequence, sequence.getLastCallId());
        assertEquals(oldTail + 1, sequence.getTail());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void complete_whenNoMatchingNext() {
        nextCallId(sequence, false);
        sequence.complete();
        sequence.complete();
    }

    static long nextCallId(CallIdSequence seq, boolean isUrgent) {
        try {
            return seq.next(isUrgent);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
