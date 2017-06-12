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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processor.Context;
import com.hazelcast.jet.StreamingTestSupport;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.function.DistributedSupplier;
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
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.processor.Processors.aggregateToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.combineToSlidingWindow;
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
    public boolean singleStageProcessor;

    private SlidingWindowP<?, ?, Long> processor;

    @Parameters(name = "hasDeduct={0}, singleStageProcessor={1}")
    public static Collection<Object[]> parameters() {
        return IntStream.range(0, 4)
                        .mapToObj(i -> new Boolean[]{(i & 2) == 2, (i & 1) == 1})
                        .collect(toList());
    }

    @Before
    public void before() {
        WindowDefinition windowDef = slidingWindowDef(4, 1);
        AggregateOperation<Entry<?, Long>, LongAccumulator, Long> operation;

        operation = AggregateOperation.of(
                    LongAccumulator::new,
                    (acc, item) -> acc.addExact(item.getValue()),
                    LongAccumulator::addExact,
                    hasDeduct ? LongAccumulator::subtractExact : null,
                    LongAccumulator::get);

        DistributedSupplier<Processor> procSupplier = singleStageProcessor
                ? aggregateToSlidingWindow(
                            t -> KEY,
                            Entry<Long, Long>::getKey,
                            TimestampKind.EVENT,
                            windowDef,
                            operation)
                : combineToSlidingWindow(windowDef, operation);
        processor = (SlidingWindowP<?, ?, Long>) procSupplier.get();
        processor.init(outbox, mock(Context.class));
    }

    @After
    public void after() {
        assertTrue("tsToKeyToFrame is not empty: " + processor.tsToKeyToAcc, processor.tsToKeyToAcc.isEmpty());
        assertTrue("slidingWindow is not empty: " + processor.slidingWindow, processor.slidingWindow.isEmpty());
    }

    @Test
    public void when_noFramesReceived_then_onlyEmitWm() {
        // Given
        inbox.addAll(singletonList(
                wm(1)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(singletonList(
                wm(1)
        ));
    }

    @Test
    public void when_receiveAscendingTimestamps_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                event(0, 1),
                event(1, 1),
                event(2, 1),
                event(3, 1),
                event(4, 1),
                wm(1),
                wm(2),
                wm(3),
                wm(4),
                wm(5),
                wm(6),
                wm(7)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                wm(1),
                outboxFrame(2, 3),
                wm(2),
                outboxFrame(3, 4),
                wm(3),
                outboxFrame(4, 4),
                wm(4),
                outboxFrame(5, 3),
                wm(5),
                outboxFrame(6, 2),
                wm(6),
                outboxFrame(7, 1),
                wm(7)
        ));
    }

    @Test
    public void when_receiveDescendingTimestamps_then_emitAscending() {
        // Given
        inbox.addAll(asList(
                event(4, 1),
                event(3, 1),
                event(2, 1),
                event(1, 1),
                event(0, 1),
                wm(1),
                wm(2),
                wm(3),
                wm(4),
                wm(5),
                wm(6),
                wm(7)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                wm(1),
                outboxFrame(2, 3),
                wm(2),
                outboxFrame(3, 4),
                wm(3),
                outboxFrame(4, 4),
                wm(4),
                outboxFrame(5, 3),
                wm(5),
                outboxFrame(6, 2),
                wm(6),
                outboxFrame(7, 1),
                wm(7)
        ));
    }

    @Test
    public void when_receiveRandomTimestamps_then_emitAscending() {
        // Given
        final List<Long> timestampsToAdd = LongStream.range(0, 100).boxed().collect(toList());
        shuffle(timestampsToAdd);
        for (long ts : timestampsToAdd) {
            inbox.add(event(ts, 1));
        }
        for (long i = 1; i <= 105; i++) {
            inbox.add(wm(i));
        }

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        List<Object> expectedOutbox = new ArrayList<>();
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(0, 1),
                outboxFrame(1, 2),
                wm(1),
                outboxFrame(2, 3),
                wm(2),
                outboxFrame(3, 4),
                wm(3)
        ));
        for (long ts = 4; ts < 100; ts++) {
            expectedOutbox.add(outboxFrame(ts, 4));
            expectedOutbox.add(wm(ts));
        }
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(100, 3),
                wm(100),
                outboxFrame(101, 2),
                wm(101),
                outboxFrame(102, 1),
                wm(102),
                wm(103),
                wm(104),
                wm(105)
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
                wm(15),
                event(16, 3),
                wm(19)
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

                wm(15),
                outboxFrame(16, 3),
                outboxFrame(17, 3),
                outboxFrame(18, 3),
                outboxFrame(19, 3),
                wm(19)
        ));
    }

    @Test
    public void when_receiveWithGaps_then_doNotSkipFrames() {
        // Given
        inbox.addAll(asList(
                event(10, 1),
                event(11, 1),
                event(12, 1),
                wm(1),
                event(2, 1),
                event(3, 1),
                event(4, 1),
                wm(4),
                wm(12),
                wm(15)
        ));

        // When
        processor.process(0, inbox);
        assertTrue(inbox.isEmpty());

        // Then
        assertOutbox(asList(
                wm(1),
                outboxFrame(2, 1),
                outboxFrame(3, 2),
                outboxFrame(4, 3),
                wm(4),
                outboxFrame(5, 3),
                outboxFrame(6, 2),
                outboxFrame(7, 1),
                outboxFrame(10, 1),

                outboxFrame(11, 2),
                outboxFrame(12, 3),
                wm(12),
                outboxFrame(13, 3),
                outboxFrame(14, 2),
                outboxFrame(15, 1),
                wm(15)
        ));
    }

    private Entry<Long, ?> event(long frameTs, long value) {
        return singleStageProcessor
                // frameTs is higher than any event timestamp in that frame;
                // therefore we generate an event with frameTs - 1
                ? entry(frameTs - 1, value)
                : new TimestampedEntry<>(frameTs, KEY, new LongAccumulator(value));
    }

    private static TimestampedEntry<Long, ?> outboxFrame(long ts, long value) {
        return new TimestampedEntry<>(ts, KEY, value);
    }
}
