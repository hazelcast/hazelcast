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

import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.internal.util.ConcurrencyDetection;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FailFastCallIdSequenceTest extends HazelcastTestSupport {

    @Test
    public void testGettersAndDefaults() {
        CallIdSequence sequence = new FailFastCallIdSequence(100, ConcurrencyDetection.createDisabled());
        assertEquals(0, sequence.getLastCallId());
        assertEquals(100, sequence.getMaxConcurrentInvocations());
    }

    @Test
    public void whenNext_thenSequenceIncrements() {
        CallIdSequence sequence = new FailFastCallIdSequence(100, ConcurrencyDetection.createDisabled());
        long oldSequence = sequence.getLastCallId();
        long result = sequence.next();
        assertEquals(oldSequence + 1, result);
        assertEquals(oldSequence + 1, sequence.getLastCallId());
    }

    @Test(expected = HazelcastOverloadException.class)
    public void next_whenNoCapacity_thenThrowException() throws InterruptedException {
        CallIdSequence sequence = new FailFastCallIdSequence(1, ConcurrencyDetection.createDisabled());

        // take the only slot available
        sequence.next();

        // this next is going to fail with an exception
        sequence.next();
    }

    @Test
    public void when_overCapacityButPriorityItem_then_noException() {
        CallIdSequence sequence = new FailFastCallIdSequence(1, ConcurrencyDetection.createDisabled());

        // take the only slot available
        assertEquals(1, sequence.next());

        assertEquals(2, sequence.forceNext());
    }

    @Test
    public void whenComplete_thenTailIncrements() {
        FailFastCallIdSequence sequence = new FailFastCallIdSequence(100, ConcurrencyDetection.createDisabled());
        sequence.next();

        long oldSequence = sequence.getLastCallId();
        long oldTail = sequence.getTail();
        sequence.complete();

        assertEquals(oldSequence, sequence.getLastCallId());
        assertEquals(oldTail + 1, sequence.getTail());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void complete_whenNoMatchingNext() {
        CallIdSequence sequence = new FailFastCallIdSequence(100, ConcurrencyDetection.createDisabled());

        sequence.next();
        sequence.complete();
        sequence.complete();
    }

}
