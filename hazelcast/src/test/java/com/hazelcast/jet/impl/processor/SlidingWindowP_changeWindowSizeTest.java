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

import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SlidingWindowP_changeWindowSizeTest {

    @Test
    public void when_shrinkSlidingWindowSize_then_noLeak() throws Exception {
        test(
                slidingWinPolicy(3, 1),
                slidingWinPolicy(2, 1),
                asList(
                        result(3, "key", 3L),
                        result(4, "key", 5L),
                        result(5, "key", 3L)
                ));
    }

    @Test
    public void test_extendSlidingWindowSize_leak() throws Exception {
        test(
                slidingWinPolicy(2, 1),
                slidingWinPolicy(3, 1),
                asList(
                        result(3, "key", 3L),
                        result(4, "key", 6L),
                        result(5, "key", 5L),
                        result(6, "key", 3L)
                ));
    }

    @Test
    public void when_incompatibleSlidingWindowChange1_then_incorrectResultButNoLeakOrFailure() throws Exception {
        test(
                tumblingWinPolicy(2),
                tumblingWinPolicy(3),
                singletonList(result(6, "key", 5L)));
    }

    @Test
    public void when_incompatibleSlidingWindowChange2_then_incorrectResultButNoLeakOrFailure() throws Exception {
        test(
                tumblingWinPolicy(3),
                tumblingWinPolicy(2),
                asList(
                        result(4, "key", 3L),
                        result(6, "key", 3L)
                ));
    }

    @Test
    public void when_multipleFramesRestoredIntoSingleFrame_then_incorrectResultButNoLeakOrFailure() throws Exception {
        test(
                tumblingWinPolicy(1),
                tumblingWinPolicy(4),
                singletonList(result(4, "key", 5L)));
    }

    private static void test(
            @Nonnull SlidingWindowPolicy policy1,
            @Nonnull SlidingWindowPolicy policy2,
            @Nullable List expectedOutboxAfterRestore
    ) throws Exception {
        SlidingWindowP p = createProcessor(policy1);
        TestOutbox outbox = new TestOutbox(new int[]{1024}, 1024);
        p.init(outbox, new TestProcessorContext());
        TestInbox inbox = new TestInbox();
        inbox.addAll(asList(0, 1, 2, 3));
        p.process(0, inbox);
        p.tryProcessWatermark(wm(2));
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);

        // take a snapshot and restore to a new processor with different window size
        p.saveToSnapshot();
        TestInbox snapshotInbox = new TestInbox();
        outbox.drainSnapshotQueueAndReset(snapshotInbox.queue(), false);

        p = createProcessor(policy2);
        p.init(outbox, new TestProcessorContext());
        for (Object o : snapshotInbox.queue()) {
            Entry e = (Entry) o;
            p.restoreFromSnapshot(e.getKey(), e.getValue());
        }
        p.finishSnapshotRestore();
        while (!p.complete()) { }
        if (expectedOutboxAfterRestore != null) {
            assertEquals(expectedOutboxAfterRestore, new ArrayList<>(outbox.queue(0)));
        }
        assertTrue("tsToKeyToAcc not empty", p.tsToKeyToAcc.isEmpty());
        assertTrue("slidingWindow not empty", p.slidingWindow == null || p.slidingWindow.isEmpty());
    }

    private static SlidingWindowP createProcessor(SlidingWindowPolicy winPolicy) {
        List<Function<Integer, String>> keyFns = singletonList((Integer t) -> "key");
        return new SlidingWindowP<>(
                keyFns,
                singletonList((Integer t) -> winPolicy.higherFrameTs(t)),
                winPolicy,
                0L,
                summingLong((Integer t) -> t),
                (start, end, key, result, isEarly) -> result(end, key, result),
                true);
    }

    private static String result(long winEnd, String key, long result) {
        return String.format("(%03d, %s: %d)", winEnd, key, result);
    }
}
