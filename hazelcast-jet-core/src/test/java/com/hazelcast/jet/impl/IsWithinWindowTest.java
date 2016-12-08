/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.ReceiverTasklet.COMPRESSED_SEQ_UNIT_LOG2;
import static com.hazelcast.jet.impl.ReceiverTasklet.RECEIVE_WINDOW_COMPRESSED;
import static com.hazelcast.jet.impl.SenderTasklet.isWithinWindow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class IsWithinWindowTest {

    static final long COMPRESSED_SEQ_UNIT = 1 << COMPRESSED_SEQ_UNIT_LOG2;

    @Test
    public void when_zero_andZero_then_withinWindow() throws Exception {
        assertTrue(isWithinWindow(0, 0));
    }

    @Test
    public void when_rwin_andZero_then_justOutsideWindow() throws Exception {
        final long rwinFullSeq = RECEIVE_WINDOW_COMPRESSED * COMPRESSED_SEQ_UNIT;
        assertTrue(isWithinWindow(rwinFullSeq - 1, 0));
        assertFalse(isWithinWindow(rwinFullSeq, 0));
    }

    /**
     * This test documents the limitation of the {@code isWithinWindow} function. If the
     * compressed sender seq is allowed to advance by more than {@code Integer.MAX_VALUE} ahead of
     * the compressed acked seq, it will overflow and erroneously report within window.
     */
    @Test
    public void when_aheadMoreThanMaxInt_then_overflowsAndReportsWithinWindow() throws Exception {
        assertFalse(isWithinWindow(0, Integer.MIN_VALUE + 1));
        assertTrue(isWithinWindow(0, Integer.MIN_VALUE));
        assertFalse(isWithinWindow(Integer.MAX_VALUE * COMPRESSED_SEQ_UNIT, 0));
        assertTrue(isWithinWindow(Integer.MAX_VALUE * COMPRESSED_SEQ_UNIT, -1));
    }

    @Test
    public void when_maxInt_andMaxInt_then_withinWindow() throws Exception {
        assertTrue(isWithinWindow(Integer.MAX_VALUE * COMPRESSED_SEQ_UNIT, Integer.MAX_VALUE));
    }

    @Test
    public void when_minIntPlusRwinMinusOne_andMaxInt_then_justOutsideWindow() throws Exception {
        final long minIntPlusRwinMinusOne_compressed = Integer.MIN_VALUE + RECEIVE_WINDOW_COMPRESSED - 1;
        final long fullSeq = minIntPlusRwinMinusOne_compressed * COMPRESSED_SEQ_UNIT;
        assertTrue(isWithinWindow(fullSeq - 1, Integer.MAX_VALUE));
        assertFalse(isWithinWindow(fullSeq, Integer.MAX_VALUE));
    }

    @Test
    public void when_minIntPlusRwin_andMinInt_then_justOutsideWindow() throws Exception {
        final long minIntPlusRwin_compressed = Integer.MIN_VALUE + RECEIVE_WINDOW_COMPRESSED;
        final long fullSeq = minIntPlusRwin_compressed * COMPRESSED_SEQ_UNIT;
        assertTrue(isWithinWindow(fullSeq - 1, Integer.MIN_VALUE));
        assertFalse(isWithinWindow(fullSeq, Integer.MIN_VALUE));
    }
}
