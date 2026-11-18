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

package com.hazelcast.jet.job;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.pipeline.PipelineTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.jet.impl.JobExecutionRecord.NO_SNAPSHOT;
import static com.hazelcast.jet.impl.JobRepository.JOB_EXECUTION_RECORDS_MAP_NAME;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class AbstractJobInitialSnapshotTest extends PipelineTestSupport {

    protected static final Duration ASSERT_TRUE_ALL_THE_TIME_TIMEOUT = Duration.ofSeconds(2);
    @Parameter(value = 1)
    public boolean regularSnapshotEnabled;

    @Parameter(value = 2)
    public ProcessingGuarantee processingGuarantee;

    @Parameters(name = "regularSnapshotEnabled={1}, guarantee={2}")
    public static List<Object[]> parameters() {
        return cartesianProduct(
            List.of(MEMBER_TEST_MODE, CLIENT_TEST_MODE),
            List.of(true, false),
            List.of(ProcessingGuarantee.EXACTLY_ONCE, ProcessingGuarantee.AT_LEAST_ONCE)
        );
    }

    @After
    public void after() {
        FailingSnapshot.disableFailing();
        BlockingSnapshot.unblockSnapshot();
        BlockingStreamProcessor.unblock();
    }

    public JobConfig getJobConfig(boolean suspendOnFailure) {
        JobConfig config = new JobConfig();
        config.setRequireSnapshotBeforeProcessing(true);
        config.setSuspendOnFailure(suspendOnFailure);
        config.setSnapshotIntervalMillis(regularSnapshotEnabled ? 1 : Duration.ofHours(12).toMillis());
        config.setProcessingGuarantee(processingGuarantee);
        return config;
    }

    public void fillMap(int size) {
        range(0, size).forEach(i -> srcMap.put("" + i, i));
    }

    void assertSnapshotId(Job job, int snapshotId) {
        IMap<Long, JobExecutionRecord> executions = member.getMap(JOB_EXECUTION_RECORDS_MAP_NAME);
        var execution = executions.get(job.getId());
        assertThat(execution).isNotNull();
        assertThat(execution.snapshotId()).isEqualTo(snapshotId);
    }

    void assertSnapshotExists(Job job) {
        IMap<Long, JobExecutionRecord> executions = member.getMap(JOB_EXECUTION_RECORDS_MAP_NAME);
        var execution = executions.get(job.getId());
        assertThat(execution).isNotNull();
        assertThat(execution.snapshotId()).isNotEqualTo(NO_SNAPSHOT);
    }

    void assertNoSnapshotExists(Job job) {
        IMap<Long, JobExecutionRecord> executions = member.getMap(JOB_EXECUTION_RECORDS_MAP_NAME);
        var execution = executions.get(job.getId());
        assertThat(execution).isNotNull();
        assertThat(execution.snapshotId()).isEqualTo(NO_SNAPSHOT);
    }

    void testJob() {
        srcMap.put("a", 1);
        assertTrueEventually(() -> assertThat(sinkList.size()).isEqualTo(srcMap.size()));
    }

    public static class BlockingSnapshot extends AbstractProcessor {

        private static final AtomicBoolean saveToSnapshot = new AtomicBoolean(true);

        @Override
        public boolean saveToSnapshot() {
            return saveToSnapshot.get();
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(ordinal, item);
        }

        public static void blockSnapshot()  {
            saveToSnapshot.set(false);
        }

        public static void unblockSnapshot()  {
            saveToSnapshot.set(true);
        }
    }

    public static class FailingSnapshot extends AbstractProcessor {

        private static final AtomicBoolean failPhase1 = new AtomicBoolean(false);
        private static final AtomicBoolean failPhase2 = new AtomicBoolean(false);

        @Override
        public boolean saveToSnapshot() {
            if (failPhase1.get()) {
                throw new RuntimeException("Snapshot failed phase 1");
            }
            return true;
        }

        public boolean snapshotCommitFinish(boolean success) {
            if (failPhase2.get()) {
                throw new RuntimeException("Snapshot failed phase 2");
            }
            return true;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return tryEmit(ordinal, item);
        }

        public static void disableFailing() {
            failPhase1.set(false);
            failPhase2.set(false);
        }

        public static void enableFailingPhase1() {
            failPhase1.set(true);
        }

        public static void enableFailingPhase2() {
            failPhase2.set(true);
        }
    }

    public static class BlockingStreamProcessor extends AbstractProcessor {

        private static final AtomicBoolean block = new AtomicBoolean(false);

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            if (block.get()) {
                return false;
            }
            return tryEmit(ordinal, item);
        }

        public static void block() {
            block.set(true);
        }

        public static void unblock() {
            block.set(false);
        }
    }


}
