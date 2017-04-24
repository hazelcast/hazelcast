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

import com.hazelcast.jet.Processor.Context;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.LongStream;

import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowPTest extends StreamingTestSupport {

    private SlidingWindowP<Object, Long, Long> processor;

    @Before
    public void before() {
        WindowDefinition windowDef = new WindowDefinition(1, 0, 4);
        WindowOperation<Object, Long, Long> operation = WindowOperation.of(
                () -> 0L,
                (acc, val) -> {
                    throw new UnsupportedOperationException();
                },
                (acc1, acc2) -> acc1 + acc2,
                null,
                acc -> acc);
        processor = slidingWindow(windowDef, operation, true).get();
        processor.init(outbox, mock(Context.class));
    }

    @Test
    public void when_noFramesReceived_then_onlyEmitPunc() {
        // Given
        inbox.addAll(asList(
                punc(1)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                punc(1),
                null
        ));
    }

    @Test
    public void when_receiveAscendingSeqs_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                frame(0, 1),
                frame(1, 1),
                frame(2, 1),
                frame(3, 1),
                frame(4, 1),
                punc(1),
                punc(2),
                punc(3),
                punc(4),
                punc(5)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                frame(0, 1),
                frame(1, 2),
                punc(1),
                frame(2, 3),
                punc(2),
                frame(3, 4),
                punc(3),
                frame(4, 4),
                punc(4),
                frame(5, 3),
                punc(5),
                null
        ));
    }

    @Test
    public void when_receiveDescendingSeqs_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                frame(4, 1),
                frame(3, 1),
                frame(2, 1),
                frame(1, 1),
                frame(0, 1),
                punc(1),
                punc(2),
                punc(3),
                punc(4),
                punc(5)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                frame(0, 1),
                frame(1, 2),
                punc(1),
                frame(2, 3),
                punc(2),
                frame(3, 4),
                punc(3),
                frame(4, 4),
                punc(4),
                frame(5, 3),
                punc(5),
                null
        ));
    }

    @Test
    public void when_receiveRandomSeqs_then_emitAscending() {
        // Given
        final List<Long> frameSeqsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(frameSeqsToAdd);
        for (long seq : frameSeqsToAdd) {
            inbox.add(frame(seq, 1));
        }
        for (long i = 1; i <= 105; i++) {
            inbox.add(punc(i));
        }

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                frame(0, 1),
                frame(1, 2),
                punc(1),
                frame(2, 3),
                punc(2),
                frame(3, 4),
                punc(3)
        ));
        for (long seq = 4; seq < 100; seq++) {
            assertEquals(frame(seq, 4), pollOutbox());
            assertEquals(punc(seq), pollOutbox());
        }
        assertOutbox(asList(
                frame(100, 3),
                punc(100),
                frame(101, 2),
                punc(101),
                frame(102, 1),
                punc(102),
                punc(103),
                punc(104),
                punc(105),
                null
        ));
        assertEquals(null, pollOutbox());
    }

    @Test
    public void when_receiveWithGaps_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                frame(0, 1),
                frame(10, 1),
                frame(11, 1),
                punc(50),
                frame(50, 3),
                punc(51)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                frame(0, 1),
                frame(1, 1),
                frame(2, 1),
                frame(3, 1),

                frame(10, 1),
                frame(11, 2),
                frame(12, 2),
                frame(13, 2),
                frame(14, 1),

                punc(50),
                frame(51, 3),
                punc(51),
                null
        ));
    }

    @Test
    public void when_receiveWithGaps_then_doNotSkipFrames() {
        // Given
        inbox.addAll(asList(
                frame(10, 1),
                frame(11, 1),
                frame(12, 1),
                punc(1),
                frame(2, 1),
                frame(3, 1),
                frame(4, 1),
                punc(4),
                punc(12)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                punc(1),
                frame(2, 1),
                frame(3, 2),
                frame(4, 3),
                punc(4),
                frame(5, 3),
                frame(6, 2),
                frame(7, 1),
                frame(10, 1),

                frame(11, 2),
                frame(12, 3),
                punc(12),
                null
        ));
    }

    private static Frame<Long, Long> frame(long seq, long value) {
        return new Frame<>(seq, 77L, value);
    }
}
