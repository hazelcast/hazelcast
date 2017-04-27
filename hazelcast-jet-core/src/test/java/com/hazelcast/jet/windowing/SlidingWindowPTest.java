/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Accumulators.MutableLong;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class SlidingWindowPTest extends StreamingTestSupport {

    private static final Long KEY = 77L;

    @Parameter
    public boolean hasDeduct;

    @Parameter(1)
    public boolean mutateAccumulator;

    @Parameter(2)
    public boolean oneStageProcessor;

    @Parameters(name = "hasDeduct={0}, mutateAccumulator={1}, oneStageProcessor={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true, true, true},
                {true, false, true},
                {false, true, true},
                {false, false, true},
                {true, true, false},
                {true, false, false},
                {false, true, false},
                {false, false, false},
        });
    }

    private WindowingProcessor<?, ?, Long> processor;

    @Before
    public void before() {
        WindowDefinition windowDef = new WindowDefinition(1, 0, 4);
        WindowOperation<Entry<?, Long>, MutableLong, Long> operation;

        operation = WindowOperation.<Entry<?, Long>, MutableLong, Long>of(
                MutableLong::new,
                (acc, val) -> {
                    acc.value += val.getValue();
                },
                (acc1, acc2) -> {
                    if (mutateAccumulator) {
                        acc1.value += acc2.value;
                        return acc1;
                    }
                    else {
                        return new MutableLong(acc1.value + acc2.value);
                    }
                },
                hasDeduct ? (acc1, acc2) -> {
                    acc1.value -= acc2.value;
                    return acc1;
                } : null,
                acc -> acc.value);


        if (oneStageProcessor) {
            processor = (WindowingProcessor<Entry<Long, Long>, ?, Long>) WindowingProcessors.
                    oneStageSlidingWindow(t -> KEY, (Entry<Long, Long> e) -> e.getKey(), windowDef, operation).get();
        } else {
            processor = (WindowingProcessor<Frame<Object, ?>, ?, Long>) slidingWindow(windowDef, operation).get();
        }
        processor.init(outbox, mock(Context.class));
    }

    @After
    public void after() {
        assertTrue("seqToKeyToFrame is not empty: " + processor.seqToKeyToFrame, processor.seqToKeyToFrame.isEmpty());
        assertTrue("slidingWindow is not empty: " + processor.slidingWindow, processor.slidingWindow.isEmpty());
    }

    @Test
    public void when_noFramesReceived_then_onlyEmitPunc() {
        // Given
        inbox.addAll(singletonList(
                punc(1)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(singletonList(
                punc(1)
        ));
    }

    @Test
    public void when_receiveAscendingSeqs_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                event(0, 1),
                event(1, 1),
                event(2, 1),
                event(3, 1),
                event(4, 1),
                punc(1),
                punc(2),
                punc(3),
                punc(4),
                punc(5),
                punc(6),
                punc(7)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                punc(1),
                outboxFrame(2, 3),
                punc(2),
                outboxFrame(3, 4),
                punc(3),
                outboxFrame(4, 4),
                punc(4),
                outboxFrame(5, 3),
                punc(5),
                outboxFrame(6, 2),
                punc(6),
                outboxFrame(7, 1),
                punc(7)
        ));
    }

    @Test
    public void when_receiveDescendingSeqs_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                event(4, 1),
                event(3, 1),
                event(2, 1),
                event(1, 1),
                event(0, 1),
                punc(1),
                punc(2),
                punc(3),
                punc(4),
                punc(5),
                punc(6),
                punc(7)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                punc(1),
                outboxFrame(2, 3),
                punc(2),
                outboxFrame(3, 4),
                punc(3),
                outboxFrame(4, 4),
                punc(4),
                outboxFrame(5, 3),
                punc(5),
                outboxFrame(6, 2),
                punc(6),
                outboxFrame(7, 1),
                punc(7)
        ));
    }

    @Test
    public void when_receiveRandomSeqs_then_emitAscending() {
        // Given
        final List<Long> frameSeqsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(frameSeqsToAdd);
        for (long seq : frameSeqsToAdd) {
            inbox.add(event(seq, 1));
        }
        for (long i = 1; i <= 105; i++) {
            inbox.add(punc(i));
        }

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        List<Object> expectedOutbox = new ArrayList<>();
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                punc(1),
                outboxFrame(2, 3),
                punc(2),
                outboxFrame(3, 4),
                punc(3)
        ));
        for (long seq = 4; seq < 100; seq++) {
            expectedOutbox.add(outboxFrame(seq, 4));
            expectedOutbox.add(punc(seq));
        }
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(100, 3),
                punc(100),
                outboxFrame(101, 2),
                punc(101),
                outboxFrame(102, 1),
                punc(102),
                punc(103),
                punc(104),
                punc(105)
        ));

        assertOutbox(expectedOutbox);
    }

    @Test
    public void when_receiveWithGaps_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                event(0, 1),
                event(10, 1),
                event(11, 1),
                punc(15),
                event(16, 3),
                punc(19)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 1),
                outboxFrame(2, 1),
                outboxFrame(3, 1),

                outboxFrame(10, 1),
                outboxFrame(11, 2),
                outboxFrame(12, 2),
                outboxFrame(13, 2),
                outboxFrame(14, 1),

                punc(15),
                outboxFrame(16, 3),
                outboxFrame(17, 3),
                outboxFrame(18, 3),
                outboxFrame(19, 3),
                punc(19)
        ));
    }

    @Test
    public void when_receiveWithGaps_then_doNotSkipFrames() {
        // Given
        inbox.addAll(asList(
                event(10, 1),
                event(11, 1),
                event(12, 1),
                punc(1),
                event(2, 1),
                event(3, 1),
                event(4, 1),
                punc(4),
                punc(12),
                punc(15)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                punc(1),
                outboxFrame(2, 1),
                outboxFrame(3, 2),
                outboxFrame(4, 3),
                punc(4),
                outboxFrame(5, 3),
                outboxFrame(6, 2),
                outboxFrame(7, 1),
                outboxFrame(10, 1),

                outboxFrame(11, 2),
                outboxFrame(12, 3),
                punc(12),
                outboxFrame(13, 3),
                outboxFrame(14, 2),
                outboxFrame(15, 1),
                punc(15)
        ));
    }

    private Entry<Long, ?> event(long seq, long value) {
        return oneStageProcessor
                // the -1 is due to discrepancy between eventSeq and frameSeq
                ? entry(seq - 1, value)
                : new Frame<>(seq, KEY, new MutableLong(value));
    }

    private static Frame<Long, ?> outboxFrame(long seq, long value) {
        return new Frame<>(seq, KEY, value);
    }
}
