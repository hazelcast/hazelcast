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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.test.TestSupport.verifyProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

@Category(ParallelJVMTest.class)
@RunWith(HazelcastParallelClassRunner.class)
public class SlidingWindowP_stage1Test {

    private static final long KEY = 77L;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private List<SlidingWindowP> suppliedProcessors = new ArrayList<>();
    private SupplierEx<Processor> supplier;

    @Before
    public void before() {
        FunctionEx<Entry<Long, Long>, Object> keyFn = x -> KEY;
        ToLongFunctionEx<Entry<Long, Long>> timestampFn = Entry::getKey;
        supplier = () -> {
            SlidingWindowP res = (SlidingWindowP) Processors.accumulateByFrameP(
                    singletonList(keyFn),
                    singletonList(timestampFn),
                    TimestampKind.EVENT,
                    slidingWinPolicy(16, 4),
                    summingLong(Entry<Long, Long>::getValue).withIdentityFinish()
            ).get();
            suppliedProcessors.add(res);
            return res;
        };
    }

    @After
    public void after() {
        for (SlidingWindowP processor : suppliedProcessors) {
            assertTrue("map not empty after emitting everything: " + processor.tsToKeyToAcc,
                    processor.tsToKeyToAcc.isEmpty());
        }
    }

    @Test
    public void smokeTest() {
        verifyProcessor(supplier)
                .disableSnapshots()
                .disableCompleteCall()
                .input(asList(
                        entry(0L, 1L), // to frame 4
                        entry(1L, 1L), // to frame 4
                        wm(4), // closes frame 4
                        entry(4L, 1L), // to frame 8
                        entry(5L, 1L), // to frame 8
                        entry(8L, 1L), // to frame 12
                        entry(8L, 1L), // to frame 12
                        wm(8), // closes frame 8
                        entry(8L, 1L), // to frame 12
                        wm(12),
                        wm(16),
                        wm(20) // closes everything
                ))
                .expectOutput(asList(
                        frame(4, 2),
                        wm(4),
                        frame(8, 2),
                        wm(8),
                        frame(12, 3),
                        wm(12),
                        wm(16),
                        wm(20)
                ));
    }

    @Test
    public void when_gapInWmAfterEvent_then_frameAndWmEmitted() {
        verifyProcessor(supplier)
                .disableSnapshots()
                .disableCompleteCall()
                .input(asList(
                        entry(0L, 1L), // to frame 4
                        entry(1L, 1L), // to frame 4
                        wm(12) // closes frame
                ))
                .expectOutput(asList(
                        frame(4, 2),
                        wm(12)
                ));
    }

    @Test
    public void when_noEvents_then_wmsEmitted() {
        List<Watermark> someWms = asList(
                wm(4),
                wm(8),
                wm(12)
        );

        verifyProcessor(supplier)
                .disableSnapshots()
                .disableCompleteCall()
                .input(someWms)
                .expectOutput(someWms);
    }

    @Test
    public void when_batch_then_emitEverything() {
        verifyProcessor(supplier)
                .disableSnapshots()
                .input(asList(
                        entry(0L, 1L), // to frame 4
                        entry(4L, 1L), // to frame 8
                        entry(8L, 1L), // to frame 12
                        wm(4) // closes frame 4
                        // no WM to emit any window, everything should be emitted in complete as if we received
                        // wm(4), wm(8), wm(12)
                ))
                .expectOutput(asList(
                        frame(4, 1),
                        wm(4),
                        frame(8, 1),
                        frame(12, 1)
                ));
    }

    @Test
    public void when_wmNeverReceived_then_emitEverythingInComplete() {
        verifyProcessor(supplier)
                .disableSnapshots()
                .input(asList(entry(0L, 1L), // to frame 4
                        entry(4L, 1L) // to frame 8
                        // no WM to emit any window, everything should be emitted in complete as if we received
                        // wm(4), wm(8)
                ))
                .expectOutput(asList(
                        frame(4, 1),
                        frame(8, 1)
                ));
    }

    @Test
    public void when_lateEvent_then_ignore() {
        verifyProcessor(supplier)
                .disableSnapshots()
                .disableCompleteCall()
                .input(asList(wm(16),
                        entry(7, 1)
                ))
                .expectOutput(singletonList(wm(16)));
    }

    private static <V> KeyedWindowResult<Long, LongAccumulator> frame(long ts, long value) {
        return new KeyedWindowResult<>(ts - 4, ts, KEY, new LongAccumulator(value));
    }
}
