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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.execution.ReceiverTasklet.COMPRESSED_SEQ_UNIT_LOG2;
import static com.hazelcast.jet.impl.execution.SenderTasklet.isWithinLimit;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsWithinLimitTest {

    static final long COMPRESSED_SEQ_UNIT = 1 << COMPRESSED_SEQ_UNIT_LOG2;

    /**
     * This test documents the limitation of the {@code isWithinLimit} function. If the
     * compressed sender seq is allowed to advance by more than {@code Integer.MAX_VALUE} ahead of
     * the compressed acked seq, it will overflow and erroneously report within limit.
     */
    @Test
    public void when_aheadMoreThanMaxInt_then_overflowsAndReportsWithinWindow() {
        assertFalse(isWithinLimit(0, Integer.MIN_VALUE + 1));
        assertTrue(isWithinLimit(0, Integer.MIN_VALUE));
        assertFalse(isWithinLimit(Integer.MAX_VALUE * COMPRESSED_SEQ_UNIT, 0));
        assertTrue(isWithinLimit(Integer.MAX_VALUE * COMPRESSED_SEQ_UNIT, -1));
    }

    @Test
    public void when_one_andZero_then_justOutsideWindow() {
        assertTrue(isWithinLimit(0, 0));
        assertFalse(isWithinLimit(COMPRESSED_SEQ_UNIT, 0));
    }

    @Test
    public void when_maxInt_andMaxInt_then_withinWindow() {
        assertTrue(isWithinLimit(Integer.MAX_VALUE * COMPRESSED_SEQ_UNIT, Integer.MAX_VALUE));
    }

    @Test
    public void when_minInt_andMaxInt_then_justOutsideWindow() {
        final long fullSeq = (long) Integer.MIN_VALUE * COMPRESSED_SEQ_UNIT;
        assertTrue(isWithinLimit(fullSeq - 1, Integer.MAX_VALUE));
        assertFalse(isWithinLimit(fullSeq, Integer.MAX_VALUE));
    }

    @Test
    public void when_minIntPlusOne_andMinInt_then_justOutsideWindow() {
        final long minIntPlusOne_compressed = Integer.MIN_VALUE + 1;
        final long fullSeq = minIntPlusOne_compressed * COMPRESSED_SEQ_UNIT;
        assertTrue(isWithinLimit(fullSeq - 1, Integer.MIN_VALUE));
        assertFalse(isWithinLimit(fullSeq, Integer.MIN_VALUE));
    }
}
