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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRepository;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PostponedSnapshotTestBase extends JetTestSupport {

    @SuppressWarnings("WeakerAccess") // used by subclass in jet-enterprise
    protected static volatile AtomicIntegerArray latches;

    protected HazelcastInstance instance;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        latches = new AtomicIntegerArray(2);
    }

    @SuppressWarnings("WeakerAccess") // used by subclass in jet-enterprise
    protected Job startJob(long snapshotInterval) {
        DAG dag = new DAG();
        Vertex highPrioritySource = dag.newVertex("highPrioritySource", () -> new SourceP(0)).localParallelism(1);
        Vertex lowPrioritySource = dag.newVertex("lowPrioritySource", () -> new SourceP(1)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeLoggerP());

        dag.edge(between(highPrioritySource, sink).priority(-1))
           .edge(from(lowPrioritySource).to(sink, 1));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(snapshotInterval);

        Job job = instance.getJet().newJob(dag, config);
        JobRepository jr = new JobRepository(instance);

        // check, that snapshot starts, but stays in ONGOING state
        if (snapshotInterval < 1000) {
            assertTrueEventually(() -> {
                JobExecutionRecord record = jr.getJobExecutionRecord(job.getId());
                assertNotNull("record is null", record);
                assertTrue(record.ongoingSnapshotId() >= 0);
            }, 5);
            assertTrueAllTheTime(() -> {
                JobExecutionRecord record = jr.getJobExecutionRecord(job.getId());
                assertTrue(record.ongoingSnapshotId() >= 0);
                assertTrue("snapshotId=" + record.snapshotId(),
                        record.snapshotId() < 0);
            }, 2);
        } else {
            assertJobStatusEventually(job, RUNNING);
        }
        return job;
    }

    private static final class SourceP extends AbstractProcessor {
        private final int latchIndex;

        SourceP(int latchIndex) {
            this.latchIndex = latchIndex;
        }

        @Override
        public boolean complete() {
            return latches.get(latchIndex) != 0;
        }

        @Override
        public boolean saveToSnapshot() {
            return tryEmitToSnapshot(latchIndex, latchIndex);
        }
    }
}
