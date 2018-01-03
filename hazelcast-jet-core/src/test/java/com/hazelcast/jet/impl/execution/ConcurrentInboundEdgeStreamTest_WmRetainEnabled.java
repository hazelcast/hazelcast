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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
public class ConcurrentInboundEdgeStreamTest_WmRetainEnabled {

    private static final Object senderGone = new Object();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private OneToOneConcurrentArrayQueue<Object> q1;
    private OneToOneConcurrentArrayQueue<Object> q2;
    private ConcurrentInboundEdgeStream stream;
    private ConcurrentConveyor<Object> conveyor;

    @Before
    public void setUp() {
        q1 = new OneToOneConcurrentArrayQueue<>(128);
        q2 = new OneToOneConcurrentArrayQueue<>(128);
        //noinspection unchecked
        conveyor = ConcurrentConveyor.concurrentConveyor(senderGone, q1, q2);

        stream = createCies(false);
    }

    private ConcurrentInboundEdgeStream createCies(boolean waitForSnapshot) {
        return new ConcurrentInboundEdgeStream(conveyor, 0, 0, -1, waitForSnapshot, 16);
    }

    @Test
    public void when_wmOnSingleQueueOnly_then_emittedAfterDelay_singleWm() {
        // When
        add(q1, wm(1));

        // Then
        drainAndAssert(0, MADE_PROGRESS);
        for (int i = 0; i < 16; i++) {
            drainAndAssert(i, NO_PROGRESS);
        }
        drainAndAssert(16, MADE_PROGRESS, wm(1));
    }

    @Test
    public void when_wmOnSingleQueueOnly_then_emittedAfterDelay_multipleWm() {
        // At time 0, 1, 2, ... we add WM 100, 101, 102 to just one queue. The maximum retain time is 16,
        // so at t=16 we should receive wm(100).
        for (int time = 0; time < 40; time++) {
            add(q1, wm(100 + time));
            Object[] expectedItems = time < 16 ? new Object[0] : new Object[]{wm(time + 100 - 16)};
            drainAndAssert(time, MADE_PROGRESS, expectedItems);
        }
    }

    @Test
    public void when_wmLate_then_ignored() {
        add(q1, wm(1));
        drainAndAssert(0, MADE_PROGRESS);
        drainAndAssert(1, NO_PROGRESS);
        // now after delay wm is forwarded despite it wasn't present on q2
        drainAndAssert(16, MADE_PROGRESS, wm(1));
        drainAndAssert(17, NO_PROGRESS);

        // When - add wm to q2 even though it already was forwarded
        add(q2, wm(1));

        // Then - WM is not drained
        drainAndAssert(17, MADE_PROGRESS);
        drainAndAssert(17, NO_PROGRESS);
    }

    @Test
    public void when_wmOnSingleQueueOnlyWithBarrier_then_barriersForwardedNormally() {
        add(q1, wm(1), barrier(0));
        add(q2, barrier(0));
        drainAndAssert(0, MADE_PROGRESS); // technicality, queue drain stops at the wm and doesn't process the barrier
        drainAndAssert(0, MADE_PROGRESS, barrier(0));
        drainAndAssert(16, MADE_PROGRESS, wm(1));
    }

    @Test
    public void when_wmOnSingleQueueOnlyWithItem_then_barriersForwardedNormally() {
        add(q1, wm(1), "item1");
        add(q2, "item2");
        drainAndAssert(0, MADE_PROGRESS, "item2");
        drainAndAssert(0, MADE_PROGRESS, "item1");
        drainAndAssert(16, MADE_PROGRESS, wm(1));
    }

    @Test
    public void when_barrierAndWmInQueues_then_notReordered1() {
        // When
        add(q1, wm(1));
        add(q2, barrier(0));
        drainAndAssert(0, MADE_PROGRESS);
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());

        add(q1, barrier(0));

        // Then
        drainAndAssert(16, MADE_PROGRESS, barrier(0));
        drainAndAssert(16, MADE_PROGRESS, wm(1));
    }

    @Test
    public void when_barrierAndWmInQueues_then_notReordered2() {
        // When
        add(q1, wm(1));
        add(q2, barrier(0));
        drainAndAssert(0, MADE_PROGRESS);
        assertEquals(0, q1.size());
        assertEquals(0, q2.size());

        add(q1, barrier(0));

        // Then
        drainAndAssert(15, MADE_PROGRESS, barrier(0));
        drainAndAssert(16, MADE_PROGRESS, wm(1));
    }

    private void drainAndAssert(long now, ProgressState expectedState, Object... expectedItems) {
        List<Object> list = new ArrayList<>();
        assertEquals("progressState", expectedState, stream.drainTo(MILLISECONDS.toNanos(now), list::add));
        assertEquals(asList(expectedItems), list);
    }

    private void add(OneToOneConcurrentArrayQueue<Object> q, Object... items) {
        q.addAll(asList(items));
    }

    private Watermark wm(long timestamp) {
        return new Watermark(timestamp);
    }

    private SnapshotBarrier barrier(long snapshotId) {
        return new SnapshotBarrier(snapshotId);
    }
}
