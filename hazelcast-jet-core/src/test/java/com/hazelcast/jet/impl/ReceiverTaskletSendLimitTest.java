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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.ReceiverTasklet.COMPRESSED_SEQ_UNIT_LOG2;
import static com.hazelcast.jet.impl.ReceiverTasklet.INITIAL_RECEIVE_WINDOW_COMPRESSED;
import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class ReceiverTaskletSendLimitTest {

    private static final long START = SECONDS.toNanos(10);
    private static final long ACK_PERIOD = MILLISECONDS.toNanos(JetService.FLOW_CONTROL_PERIOD_MS);
    private static final int RWIN_MULTIPLIER = 3;
    private static final int FLOW_CONTROL_PERIOD_MS = 100;

    ReceiverTasklet tasklet;

    @Before
    public void before() {
        tasklet = new ReceiverTasklet(null, RWIN_MULTIPLIER, FLOW_CONTROL_PERIOD_MS, 2);
    }

    @Test
    public void when_ackOneSender_then_otherSenderUnaffected() {
        // Given
        final int busySenderId = 0;

        // When
        tasklet.updateAndGetSendSeqLimitCompressed(busySenderId, START);
        tasklet.ackItem(busySenderId, INITIAL_RECEIVE_WINDOW_COMPRESSED << COMPRESSED_SEQ_UNIT_LOG2);
        tasklet.updateAndGetSendSeqLimitCompressed(busySenderId, START + ACK_PERIOD);

        // Then
        final int quietSenderId = 1;
        assertEquals(INITIAL_RECEIVE_WINDOW_COMPRESSED, tasklet.updateAndGetSendSeqLimitCompressed(quietSenderId, 0));
    }

    @Test
    public void when_noData_then_rwinHalvedEachTime() throws Exception {
        double expectedSeq = INITIAL_RECEIVE_WINDOW_COMPRESSED;
        for (int i = 0; i < 10; i++) {
            assertEquals((long) expectedSeq, tasklet.updateAndGetSendSeqLimitCompressed(0, START + i * ACK_PERIOD));
            expectedSeq = ceil(expectedSeq / 2);
        }
    }

    @Test
    public void when_steadyFlow_then_steadyRwin() throws Exception {
        // Given
        final int ackedSeqsPerIterCompressed = 1000;
        final long ackedSeqsPerIter = ackedSeqsPerIterCompressed << COMPRESSED_SEQ_UNIT_LOG2;
        final int iterCount = 15;
        long seqLimitCompressed = 0;

        // When
        for (int i = 0; i < iterCount; i++) {
            tasklet.ackItem(0, ackedSeqsPerIter);
            seqLimitCompressed = tasklet.updateAndGetSendSeqLimitCompressed(0, START + i * ACK_PERIOD);
        }

        // Then
        final long ackedSeqCompressed = (iterCount * ackedSeqsPerIter) >> COMPRESSED_SEQ_UNIT_LOG2;
        final long rwin = seqLimitCompressed - ackedSeqCompressed;
        assertTrue(abs(rwin - RWIN_MULTIPLIER * ackedSeqsPerIterCompressed) < 2);
    }

    @Test
    public void when_hiccupInReceiver_then_rwinDropsToZero() throws Exception {
        // Given
        final int ackedSeqsPerIterCompressed = 1000;
        final long ackedSeqsPerIter = ackedSeqsPerIterCompressed << COMPRESSED_SEQ_UNIT_LOG2;
        final int warmupIters = 15;
        final int hiccupIters = 15;

        int iter = 0;

        for (int i = 0; i < warmupIters; i++, iter++) {
            tasklet.ackItem(0, ackedSeqsPerIter);
            tasklet.updateAndGetSendSeqLimitCompressed(0, START + iter * ACK_PERIOD);
        }

        // When
        long seqLimit = 0;
        for (int i = 0; i < hiccupIters; i++, iter++) {
            seqLimit = tasklet.updateAndGetSendSeqLimitCompressed(0, START + iter * ACK_PERIOD);
        }

        // Then
        final long ackedSeqCompressed = (warmupIters * ackedSeqsPerIter) >> COMPRESSED_SEQ_UNIT_LOG2;
        final long rwin = seqLimit - ackedSeqCompressed;
        assertTrue(rwin == 0 || rwin == 1);
    }

    @Test
    public void when_recoverFromHiccup_then_rwinRecoversQuickly() throws Exception {
        // Given
        final int ackedSeqsPerIterCompressed = 1000;
        final long ackedSeqsPerIter = ackedSeqsPerIterCompressed << COMPRESSED_SEQ_UNIT_LOG2;
        final int warmupIters = 15;
        final int hiccupIters = 15;

        int iter = 0;
        long seqLimitBeforeHiccup = 0;
        long ackedBeforeHiccup = 0;

        for (int i = 0; i < warmupIters; i++, iter++) {
            ackedBeforeHiccup = tasklet.ackItem(0, ackedSeqsPerIter);
            seqLimitBeforeHiccup = tasklet.updateAndGetSendSeqLimitCompressed(0, START + iter * ACK_PERIOD);
        }
        for (int i = 0; i < hiccupIters; i++, iter++) {
            tasklet.updateAndGetSendSeqLimitCompressed(0, START + iter * ACK_PERIOD);
        }

        // When

        // After a hiccup all the enqueued items are processed within one ack period:
        final long recoverySize = (seqLimitBeforeHiccup << COMPRESSED_SEQ_UNIT_LOG2) - ackedBeforeHiccup;
        final long ackedAfterRecover = tasklet.ackItem(0, recoverySize);
        final int seqLimitAfterRecover = tasklet.updateAndGetSendSeqLimitCompressed(0, START + iter * ACK_PERIOD);

        // Then
        final long ackedSeqCompressed = ackedAfterRecover >> COMPRESSED_SEQ_UNIT_LOG2;
        final long rwin = seqLimitAfterRecover - ackedSeqCompressed;
        assertTrue(rwin >= RWIN_MULTIPLIER * ackedSeqsPerIterCompressed);
    }
}
