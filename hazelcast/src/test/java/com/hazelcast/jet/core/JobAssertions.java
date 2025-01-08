/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import javax.annotation.Nonnull;

import java.time.Duration;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JetTestSupport.getJetServiceBackend;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Contains assertions about some particular job.
 *
 * <p>
 *     For instance creation please use {@link JobAssertions#assertThat(Job)}.
 * </p>
 */
@SuppressWarnings("UnusedReturnValue")
public class JobAssertions extends AbstractAssert<JobAssertions, Job> {

    protected JobAssertions(Job job, Class<?> selfType) {
        super(job, selfType);
    }

    /**
     * Creates new assertions object for given job.
     */
    public static JobAssertions assertThat(Job job) {
        return new JobAssertions(job, JobAssertions.class);
    }

    /**
     * Asserts that the {@code job} has an {@link ExecutionContext} on the
     * given {@code instance}.
     */
    public JobAssertions isExecutingOn(HazelcastInstance instance) {
        var execCtx = getExecutionContext(instance);
        assertNotNull("Job should be executing on member " + instance + ", but is not", execCtx);
        return this;
    }

    /**
     * Asserts that the {@code job} does not have an {@link ExecutionContext}
     * on the given {@code instance}.
     */
    public JobAssertions isNotExecutingOn(HazelcastInstance instance) {
        var execCtx = getExecutionContext(instance);
        assertNull("Job should not be executing on member " + instance + ", but is", execCtx);
        return this;
    }

    /**
     * Asserts that the {@code job} is already visible by {@linkplain JetService#getJob} methods.
     *
     * @param client     Hazelcast Instance used to query the cluster
     */
    public JobAssertions isVisible(HazelcastInstance client) {
        assertTrueEventually("job not found",
                () -> assertNotNull(client.getJet().getJob(actual.getId())));
        return this;
    }

    private ExecutionContext getExecutionContext(HazelcastInstance instance) {
        return getJetServiceBackend(instance).getJobExecutionService().getExecutionContext(actual.getId());
    }

    public JobAssertions eventuallyHasStatus(@Nonnull JobStatus expected) {
        eventuallyHasStatus(expected, ASSERT_TRUE_EVENTUALLY_TIMEOUT_DURATION);
        return this;
    }

    public JobAssertions eventuallyHasStatus(JobStatus expected, Duration timeout) {
        assertNotNull(actual);
        String message = "jobId=" + idToString(actual.getId());
        assertTrueEventually(() ->
                assertEquals(message, expected, actual.getStatus()), timeout.getSeconds());
        return this;
    }

    /**
     * Asserts that the {@code job} is eventually SUSPENDED and waits until the suspension cause is set.
     */
    public JobAssertions eventuallySuspended() {
        assertThat(actual).eventuallyHasStatus(SUSPENDED);
        assertTrueEventually(() -> {
            try {
                assertNotNull(actual.getSuspensionCause());
            } catch (IllegalStateException notSuspended) {
                Assertions.fail("Suspension cause is not set yet", notSuspended);
            }
        });
        return this;
    }

    /**
     * Asserts that a job status is eventually RUNNING. When it's running,
     * checks that the execution ID is different from the given {@code
     * ignoredExecutionId}, if not, tries again.
     * <p>
     * This is useful when checking that the job is running after a restart:
     * <pre>{@code
     *     job.restart();
     *     // This is racy, we might see the previous execution running.
     *     // Subsequent steps can fail because the job is restarting.
     *     assertJobStatusEventually(job, RUNNING);
     * }</pre>
     * <p>
     * This method allows an equivalent code:
     * <pre>{@code
     *     long oldExecutionId = assertJobRunningEventually(instance, job, null);
     *     // now we're sure the job is safe to restart - restart fails if the job isn't running
     *     job.restart();
     *     assertJobRunningEventually(instance, job, oldExecutionId);
     *     // now we're sure that a new execution is running
     * }</pre>
     *
     * @param ignoredExecutionId If job is running and has this execution ID,
     *                           wait longer. If null, no execution ID is ignored.
     * @return the execution ID of the new execution or 0 if {@code
     * ignoredExecutionId == null}
     */
    public long eventuallyJobRunning(HazelcastInstance instance, Long ignoredExecutionId) {
        Long executionId;
        JobExecutionService service = getJetServiceBackend(instance).getJobExecutionService();
        long nullSince = Long.MIN_VALUE;
        do {
            assertThat(actual).eventuallyHasStatus(RUNNING);
            // executionId can be null if the execution just terminated
            executionId = service.getExecutionIdForJobId(actual.getId());
            if (executionId == null) {
                if (nullSince == Long.MIN_VALUE) {
                    nullSince = System.nanoTime();
                } else {
                    if (NANOSECONDS.toSeconds(System.nanoTime() - nullSince) > 10) {
                        // Because we check the execution ID, make sure the execution is running on
                        // the given instance. E.g. a job with a non-distributed source and no
                        // distributed edge will complete on all but one member immediately.
                        throw new RuntimeException("The executionId is null for 10 secs - is the job running on all members?");
                    }
                }
            } else {
                nullSince = Long.MIN_VALUE;
            }
        } while (executionId == null || executionId.equals(ignoredExecutionId));
        return executionId;
    }
}
