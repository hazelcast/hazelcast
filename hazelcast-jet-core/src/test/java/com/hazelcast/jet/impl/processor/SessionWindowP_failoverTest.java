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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.processor.SessionWindowP.Keys;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelTest.class)
public class SessionWindowP_failoverTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SessionWindowP<Entry<String, Long>, ?, Long, ?> p;

    private void init(ProcessingGuarantee guarantee) {
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();
        p = new SessionWindowP<>(
                5000,
                singletonList((DistributedToLongFunction<Entry<?, Long>>) Entry::getValue),
                singletonList(entryKey()),
                aggrOp,
                WindowResult::new);

        Outbox outbox = new TestOutbox(128);
        Context context = new TestProcessorContext()
                .setProcessingGuarantee(guarantee);
        p.init(outbox, context);
    }

    @Test
    public void when_differentWmExactlyOnce_then_fail() {
        init(EXACTLY_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 1L);
        exception.expect(AssertionError.class);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 2L);
    }

    @Test
    public void when_differentWmAtLeastOnce_then_useMin() {
        init(AT_LEAST_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 2L);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 1L);
        p.finishSnapshotRestore();

        assertEquals(1L, p.currentWatermark);
    }

    @Test
    public void when_noSnapshotRestored_then_wmIsMin() {
        init(AT_LEAST_ONCE);

        assertEquals(Long.MIN_VALUE, p.currentWatermark);
    }
}
