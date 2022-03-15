/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.sequence;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.DummyBackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.spi.impl.operationservice.impl.DummyPriorityOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        long result = CallIdSequenceWithBackpressureTest.nextCallId(sequence, operation.isUrgent());
        assertEquals(oldSequence + 1, result);
        assertEquals(oldSequence + 1, sequence.getLastCallId());
    }

    @Test
    public void whenNextRepeated_thenKeepSucceeding() {
        for (long k = 1; k < 10000; k++) {
            Assert.assertEquals(k, CallIdSequenceWithBackpressureTest.nextCallId(sequence, false));
        }
    }

    @Test
    public void complete() {
        CallIdSequenceWithBackpressureTest.nextCallId(sequence, false);
        long oldSequence = sequence.getLastCallId();
        sequence.complete();
        assertEquals(oldSequence, sequence.getLastCallId());
    }
}
