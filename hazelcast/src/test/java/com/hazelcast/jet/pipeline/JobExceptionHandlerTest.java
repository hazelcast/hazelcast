/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobAssertions;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.core.TestProcessors.MockP;
import com.hazelcast.jet.core.TopologyChangedException;
import com.hazelcast.jet.impl.exception.EnteringPassiveClusterStateException;
import com.hazelcast.jet.impl.operation.InitExecutionOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.JobStatus.NOT_RUNNING;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

@Category(QuickTest.class)
public class JobExceptionHandlerTest extends PipelineStreamTestSupport {

    private static final AtomicBoolean shouldFail = new AtomicBoolean(true);

    @Parameter(1)
    public boolean suspendOnFailure;

    @Parameters(name = "{index}: mode={0}, suspendOnFailure={1}")
    public static Iterable<Object[]> parameters() {
        List<?> parentParams = Arrays.asList(MEMBER_TEST_MODE, CLIENT_TEST_MODE);
        List<Boolean> suspendOptions = Arrays.asList(true, false);
        return cartesianProduct(parentParams, suspendOptions);
    }

    @Before
    public void before() {
        shouldFail.set(true);
        TestProcessors.reset(0);
    }

    private void jobRestartOnExceptionTest(Exception exception) {
        int size = 100;
        int failOn = size / 2;

        AtomicInteger restartCounter = new AtomicInteger();

        StreamStage<Integer> srcStage =
            streamStageFromList(range(0, size).boxed().toList());

        srcStage
            .customTransform("block", () -> new MockP().initBlocks())
            .setLocalParallelism(1)
            .rebalance()
            .map(e -> {
                if ((int) e == failOn && shouldFail.get()) {
                    shouldFail.set(false);
                    throw exception;
                }
                return e;
            })
            .writeTo(Sinks.noop());

        JobConfig config = new JobConfig()
            .setSuspendOnFailure(suspendOnFailure);

        Job job = start(config);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        job.addStatusListener(status -> {
            if (status.getNewStatus() == NOT_RUNNING) {
                restartCounter.incrementAndGet();
            }
        });

        MockP.unblockAll();

        assertThatNoException().isThrownBy(() -> job.getFuture().get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS));

        assertThat(shouldFail).isFalse();
        assertThat(restartCounter.get()).isEqualTo(1);
    }

    @Test
    public void instanceNotActiveExceptionTest() {
        jobRestartOnExceptionTest(new HazelcastInstanceNotActiveException());
    }

    @Test
    public void memberLeftExceptionTest() {
        var member = hz().getCluster().getMembers().stream().findAny().orElseThrow();
        jobRestartOnExceptionTest(new MemberLeftException(member));
    }

    @Test
    public void topologyChangedExceptionTest() {
        jobRestartOnExceptionTest(new TopologyChangedException());
    }

    @Test
    public void targetNotMemberExceptionTest() {
        jobRestartOnExceptionTest(new TargetNotMemberException(""));
    }

    @Test
    public void enteringPassiveClusterStateExceptionTest() {
        jobRestartOnExceptionTest(new EnteringPassiveClusterStateException());
    }

    @Test
    public void operationTimeoutFromInitExecutionTest() {
        jobRestartOnExceptionTest(
            new OperationTimeoutException(
                InitExecutionOperation.class.getSimpleName()
            )
        );
    }

    @Test
    public void nonRestartableException_shouldFailJob() {
        assumeThat(suspendOnFailure).isFalse();
        int size = 100;
        StreamStage<Integer> srcStage = streamStageFromList(range(0, size).boxed().toList());
        srcStage
            .rebalance()
            .map(e -> {
                throw new IllegalStateException("non restartable");
            })
            .writeTo(sink);

        assertThatThrownBy(this::execute)
            .hasRootCauseInstanceOf(IllegalStateException.class);
    }
}
