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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.shuffle;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class SlidingWindowPTest {

    private static final Long KEY = 77L;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Parameter
    public boolean hasDeduct;

    @Parameter(1)
    public boolean singleStageProcessor;

    private SupplierEx<Processor> supplier;
    private SlidingWindowP lastSuppliedProcessor;

    @Parameters(name = "hasDeduct={0}, singleStageProcessor={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{true, true},
                new Object[]{true, false},
                new Object[]{false, true},
                new Object[]{false, false}
        );
    }

    @Before
    public void before() {
        SlidingWindowPolicy winPolicy = slidingWinPolicy(4, 1);

        AggregateOperation1<Entry<?, Long>, LongAccumulator, Long> operation = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator acc, Entry<?, Long> item) -> acc.add(item.getValue()))
                .andCombine(LongAccumulator::add)
                .andDeduct(hasDeduct ? LongAccumulator::subtract : null)
                .andExportFinish(LongAccumulator::get);

        FunctionEx<?, Long> keyFn = t -> KEY;
        ToLongFunctionEx<Entry<Long, Long>> timestampFn = Entry::getKey;
        SupplierEx<Processor> procSupplier = singleStageProcessor
                ? aggregateToSlidingWindowP(
                        singletonList(keyFn),
                        singletonList(timestampFn),
                        TimestampKind.EVENT,
                        winPolicy,
                        0L,
                        operation,
                KeyedWindowResult::new)
                : combineToSlidingWindowP(winPolicy, operation, KeyedWindowResult::new);

        // new supplier to save the last supplied instance
        supplier = () -> lastSuppliedProcessor = (SlidingWindowP) procSupplier.get();
    }

    @After
    public void after() {
        assertTrue("tsToKeyToFrame is not empty: " + lastSuppliedProcessor.tsToKeyToAcc,
                lastSuppliedProcessor.tsToKeyToAcc.isEmpty());
        assertTrue("slidingWindow is not empty: " + lastSuppliedProcessor.slidingWindow,
                lastSuppliedProcessor.slidingWindow == null || lastSuppliedProcessor.slidingWindow.isEmpty());
    }

    @Test
    public void when_noFramesReceived_then_onlyEmitWm() {
        List<Watermark> wmList = singletonList(wm(1));
        verifyProcessor(supplier)
                .disableCompleteCall()
                .input(wmList)
                .expectOutput(wmList);
    }

    @Test
    public void simple_smokeTest() {
        verifyProcessor(supplier)
                .disableCompleteCall()
                .disableLogging()
                .input(asList(
                        event(0, 1),
                        wm(3)))
                .expectOutput(asList(
                        outboxFrame(0, 1),
                        outboxFrame(1, 1),
                        outboxFrame(2, 1),
                        outboxFrame(3, 1),
                        wm(3)
                ));
    }

    @Test
    public void when_receiveAscendingTimestamps_then_emitAscending() {
        verifyProcessor(supplier)
                .disableCompleteCall()
                .input(asList(
                        event(0, 1),
                        event(1, 1),
                        event(2, 1),
                        event(3, 1),
                        event(4, 1),
                        wm(0),
                        wm(1),
                        wm(2),
                        wm(3),
                        wm(4),
                        wm(5),
                        wm(6),
                        wm(7)))
                .expectOutput(asList(
                        outboxFrame(0, 1),
                        wm(0),
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
        verifyProcessor(supplier)
                .disableCompleteCall()
                .input(asList(
                        event(4, 1),
                        event(3, 1),
                        event(2, 1),
                        event(1, 1),
                        event(0, 1),
                        wm(0),
                        wm(1),
                        wm(2),
                        wm(3),
                        wm(4),
                        wm(5),
                        wm(6),
                        wm(7)
                )).expectOutput(asList(
                        outboxFrame(0, 1),
                        wm(0),
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
        ArrayList<Object> inbox = new ArrayList<>();
        for (long ts : timestampsToAdd) {
            inbox.add(event(ts, 1));
        }
        for (long i = 0; i <= 105; i++) {
            inbox.add(wm(i));
        }

        List<Object> expectedOutbox = new ArrayList<>();
        expectedOutbox.addAll(Arrays.asList(
                outboxFrame(0, 1),
                wm(0),
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
        verifyProcessor(supplier)
                .disableCompleteCall()
                .disableLogging()
                .input(inbox)
                .expectOutput(expectedOutbox);
    }

    @Test
    public void when_wmNeverReceived_then_emitEverythingInComplete() {
        verifyProcessor(supplier)
                .input(asList(
                        event(0L, 1L), // to frame 0
                        event(1L, 1L) // to frame 1
                        // no WM to emit any window, everything should be emitted in complete as if we received
                        // wm(5)
                )).expectOutput(asList(
                        outboxFrame(0, 1),
                        outboxFrame(1, 2),
                        outboxFrame(2, 2),
                        outboxFrame(3, 2),
                        outboxFrame(4, 1)
                ));
    }

    @Test
    public void when_lateEvent_then_ignored() {
        verifyProcessor(supplier)
                .input(asList(
                        wm(10),
                        // this one is late
                        event(7L, 1L),
                        // following events are "partially late" - it's still should be dropped, even though we still have
                        // frame8, where we could accumulate it
                        event(8L, 1L),
                        event(9L, 1L),
                        event(10L, 1L),
                        // this event is the first one not late
                        event(11L, 123L)
                )).expectOutput(asList(
                        wm(10),
                        outboxFrame(11L, 123L),
                        outboxFrame(12L, 123L),
                        outboxFrame(13L, 123L),
                        outboxFrame(14L, 123L)
                ));
    }

    private Entry<Long, ?> event(long frameTs, long value) {
        return singleStageProcessor
                // frameTs is higher than any event timestamp in that frame;
                // therefore we generate an event with frameTs - 1
                ? entry(frameTs - 1, value)
                : new KeyedWindowResult<>(frameTs - 4, frameTs, KEY, new LongAccumulator(value));
    }

    private static KeyedWindowResult<Long, ?> outboxFrame(long ts, long value) {
        return new KeyedWindowResult<>(ts - 4, ts, KEY, value);
    }
}
