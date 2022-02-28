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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.JobExecutionRecord.NO_SNAPSHOT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PostponedSnapshotTest extends PostponedSnapshotTestBase {

    @Test
    public void when_jobHasHigherPriorityEdge_then_noSnapshotUntilEdgeDone() {
        Job job = startJob(100);

        latches.set(0, 1);
        JobRepository jr = new JobRepository(instance);
        assertTrueEventually(() ->
                assertTrue(jr.getJobExecutionRecord(job.getId()).dataMapIndex() != NO_SNAPSHOT));

        // finish the job
        latches.set(1, 1);
        job.join();
    }

    @Test
    public void when_gracefulShutdownWhilePostponed_then_shutsDown() {
        startJob(100);

        // When
        Future shutdownFuture = spawn(() -> instance.shutdown());
        assertTrueAllTheTime(() -> assertFalse(shutdownFuture.isDone()), 3);

        // finish the job
        latches.set(0, 1);

        // Then
        assertTrueEventually(() -> assertTrue(shutdownFuture.isDone()));
    }
}
