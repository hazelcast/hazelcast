/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
public class SnapshotContextSimpleTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private SnapshotContext ssContext =
            new SnapshotContext(mock(ILogger.class), "test job", 9, ProcessingGuarantee.EXACTLY_ONCE);

    @Test
    public void when_cancelledInitially_then_cannotStartNewSnapshot() {
        /// When
        ssContext.initTaskletCount(1, 0);
        ssContext.cancel();

        // Then
        exception.expect(CancellationException.class);
        ssContext.startNewSnapshot(10, "map", false);
    }

    @Test
    public void when_cancelledAfterSnapshotDone_then_cannotStartNewSnapshot() {
        ssContext.initTaskletCount(1, 0);
        CompletableFuture<SnapshotOperationResult> future = ssContext.startNewSnapshot(10, "map", false);

        /// When
        ssContext.snapshotDoneForTasklet(1, 1, 1);
        assertTrue(future.isDone());
        ssContext.cancel();

        // Then
        exception.expect(CancellationException.class);
        ssContext.startNewSnapshot(11, "map", false);
    }

    @Test
    public void when_cancelledMidSnapshot_then_futureCompleted_and_taskletDoneSucceeds() {
        ssContext.initTaskletCount(3, 0);
        CompletableFuture<SnapshotOperationResult> future = ssContext.startNewSnapshot(10, "map", false);

        // When
        ssContext.snapshotDoneForTasklet(1, 1, 1);
        assertFalse(future.isDone());
        ssContext.cancel();

        // Then1
        assertTrue(future.isDone());

        // Then2
        ssContext.taskletDone(10, false);
    }

    @Test
    public void when_cancelledMidSnapshot_then_snapshotDoneForTaskletSucceeds() {
        ssContext.initTaskletCount(2, 0);
        CompletableFuture<SnapshotOperationResult> future = ssContext.startNewSnapshot(10, "map", false);

        // When
        ssContext.snapshotDoneForTasklet(1, 1, 1);
        assertFalse(future.isDone());
        ssContext.cancel();

        // Then
        ssContext.snapshotDoneForTasklet(1, 1, 1);
    }

    @Test
    public void test_taskletDoneWhilePostponed() {
        ssContext.initTaskletCount(2, 2);
        CompletableFuture<SnapshotOperationResult> future = ssContext.startNewSnapshot(10, "map", false);
        assertEquals(9, ssContext.activeSnapshotId());
        ssContext.taskletDone(9, true);
        assertEquals(9, ssContext.activeSnapshotId());
        ssContext.taskletDone(9, true);
        assertEquals(10, ssContext.activeSnapshotId());
        assertTrue(future.isDone());
    }
}
