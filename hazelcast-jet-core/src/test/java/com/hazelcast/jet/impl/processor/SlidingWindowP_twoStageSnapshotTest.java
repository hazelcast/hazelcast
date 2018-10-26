/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test checks the flushing of internal buffer downstream instead of saving
 * anything to snapshot in pipeline 1 out of 2.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class SlidingWindowP_twoStageSnapshotTest {

    private static final Long KEY = 77L;

    @Parameter
    public boolean simulateRestore;

    private SlidingWindowP<?, ?, ?, ?> lastSuppliedStage1Processor;
    private SlidingWindowP<?, ?, ?, ?> lastSuppliedStage2Processor;
    private DistributedSupplier<SlidingWindowP> stage1Supplier;
    private DistributedSupplier<SlidingWindowP> stage2Supplier;

    @Parameters(name = "simulateRestore={0}")
    public static Collection<Object> data() {
        return asList(true, false);
    }

    @Before
    public void before() {
        SlidingWindowPolicy windowDef = slidingWinPolicy(4, 1);

        AggregateOperation1<Entry<?, Long>, LongAccumulator, Long> aggrOp = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((LongAccumulator acc, Entry<?, Long> item) -> acc.add(item.getValue()))
                .andCombine(LongAccumulator::add)
                .andDeduct(LongAccumulator::subtract)
                .andExportFinish(LongAccumulator::get);

        DistributedSupplier<Processor> procSupplier1 = Processors.accumulateByFrameP(
                singletonList((DistributedFunction<? super Entry<Long, Long>, ?>) t -> KEY),
                singletonList((DistributedToLongFunction<? super Entry<Long, Long>>) Entry::getKey),
                TimestampKind.EVENT,
                windowDef,
                ((AggregateOperation1<? super Entry<Long, Long>, LongAccumulator, ?>) aggrOp).withIdentityFinish()
        );

        DistributedSupplier<Processor> procSupplier2 =
                combineToSlidingWindowP(windowDef, aggrOp, TimestampedEntry::fromWindowResult);

        // new supplier to save the last supplied instance
        stage1Supplier = () -> lastSuppliedStage1Processor = (SlidingWindowP<?, ?, ?, ?>) procSupplier1.get();
        stage2Supplier = () -> lastSuppliedStage2Processor = (SlidingWindowP<?, ?, ?, ?>) procSupplier2.get();
    }

    @After
    public void after() {
        assertEmptyState(lastSuppliedStage1Processor);
        assertEmptyState(lastSuppliedStage2Processor);
    }

    @Test
    public void test() throws Exception {
        SlidingWindowP stage1p1 = stage1Supplier.get();
        SlidingWindowP stage1p2 = stage1Supplier.get();
        SlidingWindowP stage2p = stage2Supplier.get();

        TestOutbox stage1p1Outbox = newOutbox();
        TestOutbox stage1p2Outbox = newOutbox();
        TestOutbox stage2Outbox = newOutbox();
        TestInbox inbox = new TestInbox();
        TestProcessorContext context = new TestProcessorContext().setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);

        stage1p1.init(stage1p1Outbox, context);
        stage1p2.init(stage1p2Outbox, context);
        stage2p.init(stage2Outbox, context);

        // process some events in the 1st stage
        assertTrue(stage1p1.tryProcess(0, entry(1L, 1L))); // entry key is time
        assertTrue(stage1p2.tryProcess(0, entry(2L, 2L)));
        assertTrue(stage1p1Outbox.queue(0).isEmpty() && stage2Outbox.queue(0).isEmpty());

        // save state in stage1
        assertTrue(stage1p1.saveToSnapshot());
        assertTrue(stage1p2.saveToSnapshot());
        assertTrue("something put to snapshot outbox in stage1",
                stage1p1Outbox.snapshotQueue().isEmpty() && stage1p2Outbox.snapshotQueue().isEmpty());
        assertEmptyState(stage1p1);
        assertEmptyState(stage1p2);
        // process normal outbox in stage2
        processStage2(stage2p, stage1p1Outbox, stage1p2Outbox, inbox);
        if (simulateRestore) {
            // create new instances for stage1
            stage1p1 = stage1Supplier.get();
            stage1p2 = stage1Supplier.get();
            stage1p1Outbox = newOutbox();
            stage1p2Outbox = newOutbox();
            stage1p1.init(stage1p1Outbox, context);
            stage1p2.init(stage1p2Outbox, context);
        }

        // process some more events in 1st stage
        assertTrue(stage1p1.tryProcess(0, entry(3L, 3L)));
        assertTrue(stage1p1.tryProcess(0, entry(4L, 4L)));

        // process flushing WM
        assertTrue(stage1p1.tryProcessWatermark(wm(10)));
        assertTrue(stage1p2.tryProcessWatermark(wm(10)));
        processStage2(stage2p, stage1p1Outbox, stage1p2Outbox, inbox);
        assertTrue(stage2p.tryProcessWatermark(wm(10)));

        // Then
        assertEquals(
                collectionToString(asList(
                        outboxFrame(2, 1),
                        outboxFrame(3, 3),
                        outboxFrame(4, 6),
                        outboxFrame(5, 10),
                        outboxFrame(6, 9),
                        outboxFrame(7, 7),
                        outboxFrame(8, 4)
                )),
                collectionToString(stage2Outbox.queue(0)));
    }

    private static void processStage2(
            SlidingWindowP p, TestOutbox stage1p1Outbox, TestOutbox stage1p2Outbox, TestInbox inbox
    ) {
        inbox.addAll(stage1p1Outbox.queue(0));
        inbox.addAll(stage1p2Outbox.queue(0));
        stage1p1Outbox.queue(0).clear();
        stage1p2Outbox.queue(0).clear();
        p.process(0, inbox);
        assertTrue(inbox.isEmpty());
    }

    private static TestOutbox newOutbox() {
        return new TestOutbox(new int[] {128}, 128);
    }

    private static void assertEmptyState(SlidingWindowP p) {
        assertTrue("tsToKeyToFrame is not empty: " + p.tsToKeyToAcc,
                p.tsToKeyToAcc.isEmpty());
        assertTrue("slidingWindow is not empty: " + p.slidingWindow,
                p.slidingWindow == null || p.slidingWindow.isEmpty());
    }

    private static Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }

    private static TimestampedEntry<Long, ?> outboxFrame(long ts, long value) {
        return new TimestampedEntry<>(ts, KEY, value);
    }

    private static String collectionToString(Collection<?> list) {
        return list.stream()
                   .map(String::valueOf)
                   .collect(Collectors.joining("\n"));
    }
}
