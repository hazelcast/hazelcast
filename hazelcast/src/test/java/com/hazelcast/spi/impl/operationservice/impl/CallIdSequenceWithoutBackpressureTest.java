package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.CallIdSequence.CallIdSequenceWithoutBackpressure;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationservice.impl.CallIdSequenceWithBackpressureTest.nextCallId;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CallIdSequenceWithoutBackpressureTest extends HazelcastTestSupport {

    CallIdSequenceWithoutBackpressure sequence = new CallIdSequenceWithoutBackpressure();

    @Test
    public void test() {
        assertEquals(0, sequence.getLastCallId());
        assertEquals(Integer.MAX_VALUE, sequence.getMaxConcurrentInvocations());
    }

    @Test
    public void testNext() {
        // regular operation
        next(new DummyOperation());
        next(new DummyOperation());

        // backup-aware operation
        next(new DummyBackupAwareOperation());
        next(new DummyBackupAwareOperation());

        //urgent operation
        next(new DummyPriorityOperation());
        next(new DummyPriorityOperation());
    }

    private void next(Operation operation) {
        long oldSequence = sequence.getLastCallId();
        long result = nextCallId(sequence, operation.isUrgent());
        assertEquals(oldSequence + 1, result);
        assertEquals(oldSequence + 1, sequence.getLastCallId());
    }

    @Test
    public void whenNextRepeated_thenKeepSucceeding() {
        for (long k = 1; k < 10000; k++) {
            assertEquals(k, nextCallId(sequence, false));
        }
    }

    @Test
    public void complete() {
        nextCallId(sequence, false);
        long oldSequence = sequence.getLastCallId();
        sequence.complete();
        assertEquals(oldSequence, sequence.getLastCallId());
    }
}
