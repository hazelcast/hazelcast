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

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotContextSimpleTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final SnapshotContext ssContext =
            new SnapshotContext(mock(ILogger.class), "test job", 9, ProcessingGuarantee.EXACTLY_ONCE);

    @Test
    public void when_cancelledInitially_then_cannotStartNewSnapshot() {
        /// When
        ssContext.initTaskletCount(1, 1, 0);
        ssContext.cancel();

        // Then
        exception.expect(CancellationException.class);
        ssContext.startNewSnapshotPhase1(10, "map", 0);
    }

    @Test
    public void when_cancelledAfterPhase1_then_cannotStartPhase2() {
        ssContext.initTaskletCount(1, 1, 0);
        CompletableFuture<SnapshotPhase1Result> future = ssContext.startNewSnapshotPhase1(10, "map", 0);

        /// When
        ssContext.phase1DoneForTasklet(1, 1, 1);
        assertTrue(future.isDone());
        ssContext.cancel();

        // Then
        exception.expect(CancellationException.class);
        ssContext.startNewSnapshotPhase2(10, true);
    }

    @Test
    public void when_cancelledMidPhase1_then_futureCompleted() {
        ssContext.initTaskletCount(3, 3, 0);
        CompletableFuture<SnapshotPhase1Result> future = ssContext.startNewSnapshotPhase1(10, "map", 0);

        // When
        ssContext.phase1DoneForTasklet(1, 1, 1);
        assertFalse(future.isDone());
        ssContext.cancel();

        // Then1
        assertTrue(future.isDone());
    }

    @Test
    public void when_cancelledMidPhase2_then_futureCompleted() {
        ssContext.initTaskletCount(1, 1, 0);
        ssContext.startNewSnapshotPhase1(10, "map", 0);
        ssContext.phase1DoneForTasklet(1, 1, 1);
        CompletableFuture<Void> future = ssContext.startNewSnapshotPhase2(10, true);

        // When
        assertFalse(future.isDone());
        ssContext.cancel();

        // Then1
        assertTrue(future.isDone());
    }

    @Test
    public void when_cancelledMidPhase1_then_phase1DoneForTaskletSucceeds() {
        ssContext.initTaskletCount(2, 2, 0);
        CompletableFuture<SnapshotPhase1Result> future = ssContext.startNewSnapshotPhase1(10, "map", 0);

        // When
        ssContext.phase1DoneForTasklet(1, 1, 1);
        assertFalse(future.isDone());
        ssContext.cancel();

        // Then
        ssContext.phase1DoneForTasklet(1, 1, 1);
        assertTrue(future.isDone());
    }

    @Test
    public void when_cancelledMidPhase2_then_phase2DoneForTaskletSucceeds() {
        ssContext.initTaskletCount(2, 1, 0);
        ssContext.startNewSnapshotPhase1(10, "map", 0);
        ssContext.phase1DoneForTasklet(1, 1, 1);
        CompletableFuture<Void> future = ssContext.startNewSnapshotPhase2(10, true);

        // When
        ssContext.phase2DoneForTasklet();
        assertFalse(future.isDone());
        ssContext.cancel();

        // Then
        ssContext.phase2DoneForTasklet();
        assertTrue(future.isDone());
    }

    @Test
    public void test_taskletDoneWhilePostponed() {
        ssContext.initTaskletCount(2, 2, 2);
        CompletableFuture<SnapshotPhase1Result> future = ssContext.startNewSnapshotPhase1(10, "map", 0);
        assertEquals(9, ssContext.activeSnapshotIdPhase1());
        ssContext.storeSnapshotTaskletDone(9, true);
        assertEquals(9, ssContext.activeSnapshotIdPhase1());
        ssContext.storeSnapshotTaskletDone(9, true);
        assertEquals(10, ssContext.activeSnapshotIdPhase1());
        assertTrue(future.isDone());
    }

    @Test
    public void test_taskletDoneWhileInPhase1() {
        ssContext.initTaskletCount(1, 1, 0);
        CompletableFuture<SnapshotPhase1Result> future = ssContext.startNewSnapshotPhase1(10, null, 0);
        ssContext.storeSnapshotTaskletDone(9, false);
        ssContext.processorTaskletDone(9);
        assertTrue(future.isDone());
    }

    @Test
    public void test_taskletDoneWhileInPhase2() {
        ssContext.initTaskletCount(1, 1, 0);
        ssContext.startNewSnapshotPhase1(10, "map", 0);
        ssContext.phase1DoneForTasklet(1, 2, 3);
        ssContext.startNewSnapshotPhase2(10, true);
        assertThatThrownBy(() -> ssContext.processorTaskletDone(9))
                .isInstanceOf(AssertionError.class);
    }
}
