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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.operation.SnapshotOperation.SnapshotOperationResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class SnapshotContextTest {

    @Parameter
    public SnapshotStarted snapshotStarted;

    @Parameter(1)
    public int taskletCount;

    @Parameter(2)
    public TaskletDone taskletDone;

    @Parameter(3)
    public int numHigherPriority;

    @Parameters(name = "snapshotStarted={0}, taskletCount={1}, taskletDone={2}, numHigherPriority={3}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        for (SnapshotStarted snapshotStarted : SnapshotStarted.values()) {
            for (int taskletCount = 1; taskletCount <= 2; taskletCount++) {
                for (TaskletDone taskletDone : TaskletDone.values()) {
                    for (int numHigherPriority = 0; numHigherPriority <= 1; numHigherPriority++) {
                        if (numHigherPriority > 0 && taskletDone == TaskletDone.DONE_AFTER_CURRENT_SNAPSHOT
                                || snapshotStarted == SnapshotStarted.AFTER
                                        && taskletDone != TaskletDone.DONE_BEFORE_CURRENT_SNAPSHOT) {
                            // these scenarios are not allowed
                            continue;
                        }
                        res.add(new Object[]{snapshotStarted, taskletCount, taskletDone, numHigherPriority});
                    }
                }
            }
        }
        return res;
    }

    @Test
    public void test_snapshotStartedAndDone() {
        SnapshotContext ssContext =
                new SnapshotContext(mock(ILogger.class), 1, "test job", 9, ProcessingGuarantee.EXACTLY_ONCE);

        ssContext.initTaskletCount(taskletCount, numHigherPriority);
        CompletableFuture<SnapshotOperationResult> future = null;
        if (snapshotStarted == SnapshotStarted.BEFORE) {
            future = ssContext.startNewSnapshot(10, "map", false);
            assertEquals("activeSnapshotId initially", numHigherPriority > 0 ? 9 : 10, ssContext.activeSnapshotId());
        }

        if (taskletDone == TaskletDone.NOT_DONE) {
            ssContext.snapshotDoneForTasklet(0, 0, 0);
        } else if (taskletDone == TaskletDone.DONE_BEFORE_CURRENT_SNAPSHOT) {
            ssContext.taskletDone(9, numHigherPriority > 0);
        } else if (taskletDone == TaskletDone.DONE_AFTER_CURRENT_SNAPSHOT) {
            ssContext.snapshotDoneForTasklet(0, 0, 0);
            ssContext.taskletDone(10, numHigherPriority > 0);
        }

        if (snapshotStarted == SnapshotStarted.AFTER) {
            future = ssContext.startNewSnapshot(10, "map", false);
        }

        assertNotNull("future == null", future);
        assertTrue("future.isDone() == " + future.isDone(),
                future.isDone() == (taskletCount == 1));
        assertEquals("numRemainingTasklets", taskletCount - 1, ssContext.getNumRemainingTasklets().get());
        assertEquals("activeSnapshotId at the end",
                taskletDone == TaskletDone.NOT_DONE && numHigherPriority > 0 ? 9 : 10, ssContext.activeSnapshotId());
    }

    private enum SnapshotStarted {
        BEFORE,
        AFTER
    }
    private enum TaskletDone {
        NOT_DONE,
        DONE_BEFORE_CURRENT_SNAPSHOT,
        DONE_AFTER_CURRENT_SNAPSHOT
    }
}
