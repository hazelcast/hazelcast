/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TestProcessors.MockPS;
import com.hazelcast.jet.core.TestProcessors.NoOutputSourceP;
import com.hazelcast.jet.core.processor.Processors;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SuspendExecutionOnFailureTest extends TestInClusterSupport {

    private static final Throwable MOCK_ERROR = new AssertionError("mock error");

    private JobConfig jobConfig;

    @Before
    public void before() {
        jobConfig = new JobConfig().setSuspendOnFailure(true);
        TestProcessors.reset(0);
    }

    @Test
    public void when_jobRunning_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        Job job = jet().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");

        cancelAndJoin(job);
    }

    @Test
    public void when_jobCompleted_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", Processors.noopP()));
        Job job = jet().newJob(dag, jobConfig);

        // When
        job.join();
        assertEquals(job.getStatus(), COMPLETED);

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");
    }

    @Test
    public void when_jobSuspendedByUser_then_suspensionCauseSaysSo() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));

        // When
        Job job = jet().newJob(dag, jobConfig);
        assertJobStatusEventually(job, RUNNING);
        job.suspend();
        assertJobStatusEventually(job, SUSPENDED);
        assertEquals("Requested by user", job.getSuspensionCause());

        cancelAndJoin(job);
    }

    @Test
    public void when_jobSuspendedDueToFailure_then_suspensionCauseDescribeProblem() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty", () -> new MockP().setCompleteError(MOCK_ERROR));

        // When
        Job job = jet().newJob(dag, jobConfig);

        // Then
        assertJobStatusEventually(job, JobStatus.SUSPENDED);
        assertTrue(job.getSuspensionCause().matches("(?s)Execution failure:\n" +
                "com.hazelcast.jet.JetException: Exception in ProcessorTasklet\\{faulty#[0-9]*}: " +
                "java.lang.AssertionError: mock error.*"));

        cancelAndJoin(job);
    }

}
