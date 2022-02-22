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

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.impl.processor.SessionWindowP.Keys;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class SessionWindowP_failoverTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SessionWindowP<Entry<String, Long>, ?, Long, ?> p;

    private void init(ProcessingGuarantee guarantee) throws Exception {
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();
        p = new SessionWindowP<>(
                5000,
                0L,
                singletonList((ToLongFunctionEx<Entry<?, Long>>) Entry::getValue),
                singletonList(entryKey()),
                aggrOp,
                KeyedWindowResult::new);

        Outbox outbox = new TestOutbox(128);
        Context context = new TestProcessorContext()
                .setProcessingGuarantee(guarantee);
        p.init(outbox, context);
    }

    @Test
    public void when_differentWmExactlyOnce_then_fail() throws Exception {
        init(EXACTLY_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 1L);
        exception.expect(AssertionError.class);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 2L);
    }

    @Test
    public void when_differentWmAtLeastOnce_then_useMin() throws Exception {
        init(AT_LEAST_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 2L);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.CURRENT_WATERMARK), 1L);
        p.finishSnapshotRestore();

        assertEquals(1L, p.currentWatermark);
    }

    @Test
    public void when_noSnapshotRestored_then_wmIsMin() throws Exception {
        init(AT_LEAST_ONCE);

        assertEquals(Long.MIN_VALUE, p.currentWatermark);
    }
}
