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

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.config.InstanceConfig.DEFAULT_FLOW_CONTROL_PERIOD_MS;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.COMPRESSED_SEQ_UNIT_LOG2;
import static com.hazelcast.jet.impl.execution.ReceiverTasklet.INITIAL_RECEIVE_WINDOW_COMPRESSED;
import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReceiverTaskletSendLimitTest {

    private static final long START = SECONDS.toNanos(10);
    private static final long ACK_PERIOD = MILLISECONDS.toNanos(DEFAULT_FLOW_CONTROL_PERIOD_MS);
    private static final int RWIN_MULTIPLIER = 3;
    private static final int FLOW_CONTROL_PERIOD_MS = 100;

    private ReceiverTasklet tasklet;

    @Before
    public void before() {
        tasklet = new ReceiverTasklet(null,
                new DefaultSerializationServiceBuilder().build(),
                RWIN_MULTIPLIER, FLOW_CONTROL_PERIOD_MS,
                new LoggingServiceImpl(null, null, BuildInfoProvider.getBuildInfo(), false, null),
                new Address(), 0, "", null, "");
    }

    @Test
    public void when_noData_then_rwinRemainsUnchanged() {
        double expectedSeq = INITIAL_RECEIVE_WINDOW_COMPRESSED;
        for (int i = 0; i < 10; i++) {
            assertEquals((long) expectedSeq, tasklet.updateAndGetSendSeqLimitCompressed(START + i * ACK_PERIOD, null));
            expectedSeq = ceil(expectedSeq);
        }
    }

    @Test
    public void when_steadyFlow_then_steadyRwin() {
        // Given
        final int ackedSeqsPerIterCompressed = 1000;
        final long ackedSeqsPerIter = ackedSeqsPerIterCompressed << COMPRESSED_SEQ_UNIT_LOG2;
        final int iterCount = 15;
        long seqLimitCompressed = 0;

        // When
        for (int i = 0; i < iterCount; i++) {
            tasklet.ackItem(ackedSeqsPerIter);
            seqLimitCompressed = tasklet.updateAndGetSendSeqLimitCompressed(START + i * ACK_PERIOD, null);
        }

        // Then
        final long ackedSeqCompressed = (iterCount * ackedSeqsPerIter) >> COMPRESSED_SEQ_UNIT_LOG2;
        final long rwin = seqLimitCompressed - ackedSeqCompressed;
        assertTrue(abs(rwin - RWIN_MULTIPLIER * ackedSeqsPerIterCompressed) < 2);
    }

    @Test
    public void when_hiccupInReceiver_then_rwinDropsToZero() {
        // Given
        final int ackedSeqsPerIterCompressed = 1000;
        final long ackedSeqsPerIter = ackedSeqsPerIterCompressed << COMPRESSED_SEQ_UNIT_LOG2;
        final int warmupIters = 15;
        final int hiccupIters = 15;

        int iter = 0;

        for (int i = 0; i < warmupIters; i++, iter++) {
            tasklet.ackItem(ackedSeqsPerIter);
            tasklet.updateAndGetSendSeqLimitCompressed(START + iter * ACK_PERIOD, null);
        }

        // When
        tasklet.setNumWaitingInInbox(1);
        long seqLimit = 0;
        for (int i = 0; i < hiccupIters; i++, iter++) {
            seqLimit = tasklet.updateAndGetSendSeqLimitCompressed(START + iter * ACK_PERIOD, null);
        }

        // Then
        final long ackedSeqCompressed = (warmupIters * ackedSeqsPerIter) >> COMPRESSED_SEQ_UNIT_LOG2;
        final long rwin = seqLimit - ackedSeqCompressed;
        assertTrue("rwin=" + rwin, rwin == 0 || rwin == 1);
    }

    @Test
    public void when_recoverFromHiccup_then_rwinRecoversQuickly() {
        // Given
        final int ackedSeqsPerIterCompressed = 1000;
        final long ackedSeqsPerIter = ackedSeqsPerIterCompressed << COMPRESSED_SEQ_UNIT_LOG2;
        final int warmupIters = 15;
        final int hiccupIters = 15;

        int iter = 0;
        long seqLimitBeforeHiccup = 0;
        long ackedBeforeHiccup = 0;

        for (int i = 0; i < warmupIters; i++, iter++) {
            ackedBeforeHiccup = tasklet.ackItem(ackedSeqsPerIter);
            seqLimitBeforeHiccup = tasklet.updateAndGetSendSeqLimitCompressed(START + iter * ACK_PERIOD, null);
        }
        for (int i = 0; i < hiccupIters; i++, iter++) {
            tasklet.updateAndGetSendSeqLimitCompressed(START + iter * ACK_PERIOD, null);
        }

        // When

        // After a hiccup all the enqueued items are processed within one ack period:
        final long recoverySize = (seqLimitBeforeHiccup << COMPRESSED_SEQ_UNIT_LOG2) - ackedBeforeHiccup;
        final long ackedAfterRecover = tasklet.ackItem(recoverySize);
        final int seqLimitAfterRecover = tasklet.updateAndGetSendSeqLimitCompressed(START + iter * ACK_PERIOD, null);

        // Then
        final long ackedSeqCompressed = ackedAfterRecover >> COMPRESSED_SEQ_UNIT_LOG2;
        final long rwin = seqLimitAfterRecover - ackedSeqCompressed;
        assertTrue(rwin >= RWIN_MULTIPLIER * ackedSeqsPerIterCompressed);
    }
}
