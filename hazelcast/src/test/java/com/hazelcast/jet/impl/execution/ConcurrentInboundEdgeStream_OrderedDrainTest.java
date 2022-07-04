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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.internal.util.concurrent.ConcurrentConveyor;
import com.hazelcast.internal.util.concurrent.OneToOneConcurrentArrayQueue;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.core.JetTestSupport.wm;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static com.hazelcast.jet.impl.util.ProgressState.DONE;
import static com.hazelcast.jet.impl.util.ProgressState.MADE_PROGRESS;
import static com.hazelcast.jet.impl.util.ProgressState.NO_PROGRESS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@Category(ParallelJVMTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class ConcurrentInboundEdgeStream_OrderedDrainTest {

    private static final Object senderGone = new Object();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private OneToOneConcurrentArrayQueue<Object> q1;
    private OneToOneConcurrentArrayQueue<Object> q2;
    private InboundEdgeStream stream;

    @Before
    public void setUp() {
        q1 = new OneToOneConcurrentArrayQueue<>(128);
        q2 = new OneToOneConcurrentArrayQueue<>(128);
        ConcurrentConveyor<Object> conveyor = ConcurrentConveyor.concurrentConveyor(senderGone, q1, q2);

        stream = ConcurrentInboundEdgeStream.create(conveyor, 0, 0, false, "cies", ComparatorEx.naturalOrder());
    }

    @Test
    public void when_twoEmittersOneDoneFirst_then_madeProgress() {
        add(q1, 1, 2, DONE_ITEM);
        add(q2, 6);
        drainAndAssert(MADE_PROGRESS, 1, 2, 6);

        add(q2, 7, DONE_ITEM);
        drainAndAssert(DONE, 7);
    }

    @Test
    public void when_twoEmittersDrainedAtOnce_then_firstCallDone() {
        add(q1, 1, 2, DONE_ITEM);
        add(q2, 6, DONE_ITEM);
        // emitter1 returned 1 and 2; emitter2 returned 6
        // both are now done
        drainAndAssert(DONE, 1, 2, 6);
    }

    @Test
    public void when_allEmittersInitiallyDone_then_firstCallDone() {
        q1.add(DONE_ITEM);
        q2.add(DONE_ITEM);
        drainAndAssert(DONE);
    }

    @Test
    public void when_oneEmitterWithNoProgress_then_noProgress() {
        add(q2, 1, DONE_ITEM);
        drainAndAssert(NO_PROGRESS);
        drainAndAssert(NO_PROGRESS);

        // now make emitter1 done, without returning anything
        q1.add(DONE_ITEM);

        drainAndAssert(DONE, 1);
    }

    @Test
    public void when_receivingWatermarks_then_fail() {
        add(q1, wm(1));
        assertThatThrownBy(() -> drainAndAssert(MADE_PROGRESS))
                .hasMessageContaining("Unexpected item observed: Watermark");
    }

    @Test
    public void when_receivingBarriers_then_fail() {
        add(q1, barrier(1));
        assertThatThrownBy(() -> drainAndAssert(NO_PROGRESS))
                .hasMessageContaining("Unexpected item observed: SnapshotBarrier");
    }

    @Test
    public void when_oneQueueDone_then_theOtherWorks() {
        add(q1, DONE_ITEM);
        drainAndAssert(MADE_PROGRESS);

        add(q2, 1);
        drainAndAssert(MADE_PROGRESS, 1);

        add(q2, 2);
        drainAndAssert(MADE_PROGRESS, 2);
    }

    @Test
    public void when_disorder_then_throw() {
        add(q1, 2, 1);
        add(q2, 3);
        assertThatThrownBy(() -> drainAndAssert(DONE))
                .hasMessageContaining("Disorder on a monotonicOrder edge");
    }

    private void drainAndAssert(ProgressState expectedState, Object... expectedItems) {
        List<Object> list = new ArrayList<>();
        assertEquals("progressState", expectedState, stream.drainTo(list::add));
        assertEquals(Arrays.asList(expectedItems), list);
    }

    private void add(OneToOneConcurrentArrayQueue<Object> q, Object... items) {
        q.addAll(Arrays.asList(items));
    }

    private SnapshotBarrier barrier(long snapshotId) {
        return new SnapshotBarrier(snapshotId, false);
    }
}
