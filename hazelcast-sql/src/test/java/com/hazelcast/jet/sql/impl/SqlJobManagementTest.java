/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.core.TestUtil.createMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SqlJobManagementTest extends SqlTestSupport {

    private static final String COMPLETED_JOB_NAME = "completedJob";

    private static SqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void when_streamingDmlWithoutCreateJob_then_fail() {
        createMapping("dest", Long.class, Long.class);

        assertThatThrownBy(() -> sqlService.execute("SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))"))
                .hasMessageContaining("You must use CREATE JOB statement for a streaming DML query");
    }

    @Test
    public void when_ddlStatementWithCreateJob_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB job AS CREATE MAPPING src TYPE TestStream"))
                .hasMessageContaining("Encountered \"CREATE\" at line 1, column 19");
    }

    @Test
    public void when_dqlStatementWithCreateJob_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB job AS SELECT 42 FROM my_map"))
                .hasMessageContaining("Encountered \"SELECT\" at line 1, column 19." + System.lineSeparator() +
                        "Was expecting one of:" + System.lineSeparator() +
                        "    \"INSERT\" ..." + System.lineSeparator() +
                        "    \"SINK\" ...");
    }

    @Test
    public void when_createOrReplaceJob_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE JOB fooJob AS INSERT INTO t1 SELECT * FROM t2"))
                .hasMessageContaining("OR REPLACE is not supported for CREATE JOB");
    }

    @Test
    public void when_createJobUnknownOption_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS ('badOption'='value') AS "
                        + "INSERT INTO t1 VALUES(1)"))
                .hasMessage("From line 1, column 25 to line 1, column 35: Unknown job option: badOption");
    }

    @Test
    public void when_createJobDuplicateOption_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS ('autoScaling'='false', 'autoScaling'='false') AS "
                + "INSERT INTO t1 VALUES(1)"))
                .hasMessageContaining("Option 'autoScaling' specified more than once");
    }

    @Test
    public void when_snapshotIntervalNotNumber_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS ('snapshotIntervalMillis'='foo') AS "
                        + "INSERT INTO t1 VALUES(1)"))
               .hasMessage("From line 1, column 50 to line 1, column 54: Invalid number for snapshotIntervalMillis: foo");
    }

    @Test
    public void when_badProcessingGuarantee_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS ('processingGuarantee'='foo') AS "
                        + "INSERT INTO t1 VALUES(1)"))
               .hasMessage("From line 1, column 47 to line 1, column 51: Unsupported value for processingGuarantee: foo");
    }

    @Test
    public void testJobSubmitAndCancel() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        assertNotNull("job doesn't exist", instance().getJet().getJob("testJob"));

        sqlService.execute("DROP JOB testJob");
    }

    @Test
    public void when_duplicateName_then_fails() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        assertThatThrownBy(() ->
                sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))"))
                .hasMessageContaining("Another active job with equal name (testJob) exists");
    }

    @Test
    public void when_duplicateName_and_ifNotExists_then_secondSubmissionIgnored() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");
        assertEquals(1, countActiveJobs());

        sqlService.execute("CREATE JOB IF NOT EXISTS testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");
        assertEquals(1, countActiveJobs());
    }

    @Test
    public void when_dropNonExistingJob_then_fail() {
        assertThatThrownBy(() ->
                sqlService.execute("DROP JOB nonExistingJob"))
                .hasMessageContaining("Job doesn't exist: nonExistingJob");
    }

    @Test
    public void when_dropNonExistingJob_and_ifExists_then_ignore() {
        sqlService.execute("DROP JOB IF EXISTS nonExistingJob");
    }

    @Test
    public void when_dropCompletedJob_then_fail() {
        createCompletedJob();

        assertThatThrownBy(() ->
                sqlService.execute("DROP JOB " + COMPLETED_JOB_NAME))
                .hasMessage("Job already terminated: " + COMPLETED_JOB_NAME);
    }

    @Test
    public void when_dropCompletedJob_and_ifExists_then_ignore() {
        createCompletedJob();

        sqlService.execute("DROP JOB IF EXISTS " + COMPLETED_JOB_NAME);
    }

    @Test
    public void when_dropJobWithParameters_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("DROP JOB j", "param"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("DROP JOB does not support dynamic parameters");
    }

    @Test
    public void test_jobOptions() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob " +
                "OPTIONS (" +
                // we use non-default value for each config option
                "'processingGuarantee'='exactlyOnce'," +
                "'snapshotIntervalMillis'='6000'," +
                "'autoScaling'='false'," +
                "'splitBrainProtectionEnabled'='true'," +
                "'metricsEnabled'='false'," +
                "'initialSnapshotName'='fooSnapshot'," +
                "'storeMetricsAfterJobCompletion'='true'," +
                "'maxProcessorAccumulatedRecords'='10')" +
                "AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        JobConfig config = instance().getJet().getJob("testJob").getConfig();

        assertEquals(EXACTLY_ONCE, config.getProcessingGuarantee());
        assertEquals(6000, config.getSnapshotIntervalMillis());
        assertFalse("isAutoScaling", config.isAutoScaling());
        assertTrue("isSplitBrainProtectionEnabled", config.isSplitBrainProtectionEnabled());
        assertFalse("isMetricsEnabled", config.isMetricsEnabled());
        assertEquals("fooSnapshot", config.getInitialSnapshotName());
        assertEquals(10L, config.getMaxProcessorAccumulatedRecords());
    }

    @Test
    public void test_insert() {
        TestBatchSqlConnector.create(sqlService, "src", 3);
        createMapping("dest", Integer.class, String.class);

        sqlService.execute("CREATE JOB testJob AS INSERT INTO dest SELECT v * 2, 'value-' || v FROM src WHERE v < 2");
        assertJobStatusEventually(instance().getJet().getJob("testJob"), COMPLETED);

        assertMapEventually(
                "dest",
                "SELECT * FROM dest",
                createMap(0, "value-0", 2, "value-1")
        );
    }

    @Test
    public void test_insertFromValues() {
        createMapping("dest", Integer.class, String.class);

        sqlService.execute("CREATE JOB testJob AS INSERT INTO dest SELECT * FROM (VALUES (1, '1'))");
        assertJobStatusEventually(instance().getJet().getJob("testJob"), COMPLETED);

        assertMapEventually(
                "dest",
                "SELECT * FROM dest",
                createMap(1, "1")
        );
    }

    @Test
    public void test_sink() {
        TestBatchSqlConnector.create(sqlService, "src", 3);
        createMapping("dest", Integer.class, String.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v * 2, 'value-' || v FROM src WHERE v > 0");
        assertJobStatusEventually(instance().getJet().getJob("testJob"), COMPLETED);

        assertMapEventually(
                "dest",
                "SELECT * FROM dest",
                createMap(2, "value-1", 4, "value-2")
        );
    }

    @Test
    public void test_sinkFromValues() {
        createMapping("dest", Integer.class, String.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT * FROM (VALUES (1, '1'), (2, '2'))");
        assertJobStatusEventually(instance().getJet().getJob("testJob"), COMPLETED);

        assertMapEventually(
                "dest",
                "SELECT * FROM dest",
                createMap(1, "1", 2, "2")
        );
    }

    @Test
    public void test_dynamicParameters() {
        TestBatchSqlConnector.create(sqlService, "src", 3);
        createMapping("dest", Integer.class, String.class);

        sqlService.execute(
                "CREATE JOB testJob AS SINK INTO dest SELECT v * ?, ? || v FROM src WHERE v > ?",
                2, "value-", 0
        );

        assertMapEventually(
                "dest",
                "SELECT * FROM dest",
                createMap(2, "value-1", 4, "value-2")
        );
    }

    @Test
    public void when_clientDisconnects_then_jobContinues() {
        HazelcastInstance client = factory().newHazelcastClient();
        SqlService sqlService = client.getSql();

        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");
        Job job = instance().getJet().getJob("testJob");
        assertNotNull(job);
        assertJobStatusEventually(job, RUNNING);

        // When
        client.shutdown();
        sleepSeconds(1);

        // Then
        assertEquals(RUNNING, job.getStatus());
    }

    @Test
    public void test_suspendResume() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        Job job = instance().getJet().getJob("testJob");
        long executionId = assertJobRunningEventually(instance(), job, null);

        sqlService.execute("ALTER JOB testJob SUSPEND");
        assertJobStatusEventually(job, SUSPENDED);

        sqlService.execute("ALTER JOB testJob RESUME");
        executionId = assertJobRunningEventually(instance(), job, executionId);

        sqlService.execute("ALTER JOB testJob RESTART");
        assertJobRunningEventually(instance(), job, executionId);
    }

    @Test
    public void when_suspendResumeNonExistingJob_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("ALTER JOB foo SUSPEND"))
                .hasMessageContaining("The job 'foo' doesn't exist");

        assertThatThrownBy(() -> sqlService.execute("ALTER JOB foo RESUME"))
                .hasMessageContaining("The job 'foo' doesn't exist");

        assertThatThrownBy(() -> sqlService.execute("ALTER JOB foo RESTART"))
                .hasMessageContaining("The job 'foo' doesn't exist");
    }

    @Test
    public void when_suspendResumeCompletedJob_then_fail() {
        createCompletedJob();

        assertThatThrownBy(() -> sqlService.execute("ALTER JOB " + COMPLETED_JOB_NAME + " SUSPEND"))
                .hasMessageMatching("Cannot SUSPEND_GRACEFUL job [0-9a-f\\-]{19} because it already has a result: .*");

        assertThatThrownBy(() -> sqlService.execute("ALTER JOB " + COMPLETED_JOB_NAME + " RESUME"))
                .hasMessage("Job already completed");

        assertThatThrownBy(() -> sqlService.execute("ALTER JOB " + COMPLETED_JOB_NAME + " RESTART"))
                .hasMessageMatching("Cannot RESTART_GRACEFUL job [0-9a-f\\-]{19} because it already has a result: .*");
    }

    @Test
    public void when_alterJobWithParameters_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("ALTER JOB j SUSPEND", "param"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("ALTER JOB does not support dynamic parameters");
    }

    @Test
    public void when_snapshotExportWithoutOrReplace_then_orReplaceRequired() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        assertThatThrownBy(() -> sqlService.execute("CREATE SNAPSHOT mySnapshot FOR JOB testJob"))
                .hasMessageContaining("The OR REPLACE option is required for CREATE SNAPSHOT");
    }

    @Test
    public void when_createSnapshotWithParameters_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE SNAPSHOT s FOR JOB j", "param"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("CREATE SNAPSHOT does not support dynamic parameters");
    }

    @Test
    public void when_snapshotExport_then_failNotEnterprise() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE SNAPSHOT mySnapshot FOR JOB testJob"))
                .hasMessageContaining("You need Hazelcast Enterprise to use this feature");
    }

    @Test
    public void when_snapshotExport_jobDoesNotExist_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE SNAPSHOT mySnapshot FOR JOB nonExistentJob"))
                .hasMessageContaining("The job 'nonExistentJob' doesn't exist");
    }

    @Test
    public void when_dropSnapshotWithParameters_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("DROP SNAPSHOT s", "param"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessage("DROP SNAPSHOT does not support dynamic parameters");
    }

    @Test
    public void when_dropJobWithSnapshot_then_failNotEnterprise() {
        createMapping("dest", Long.class, Long.class);

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");

        assertThatThrownBy(() -> sqlService.execute("DROP JOB testJob WITH SNAPSHOT mySnapshot"))
                .hasMessageContaining("You need Hazelcast Enterprise to use this feature");
    }

    @Test
    public void test_createDropJobInQuickSuccession() {
        createMapping("dest", Long.class, Long.class);

        for (int i = 0; i < 10; i++) {
            sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM TABLE(GENERATE_STREAM(100))");
            sqlService.execute("DROP JOB testJob");
        }
    }

    @Test
    public void test_planCache() {
        createMapping("target", Long.class, Long.class);
        sqlService.execute("CREATE JOB job AS SINK INTO target SELECT v, v FROM TABLE(GENERATE_STREAM(100))");
        assertThat(planCache(instance()).size()).isEqualTo(1);

        sqlService.execute("DROP MAPPING target");
        assertThat(planCache(instance()).size()).isZero();
    }

    private void createCompletedJob() {
        TestBatchSqlConnector.create(sqlService, "t", 1);
        createMapping("m", Integer.class, Integer.class);
        sqlService.execute("create job " + COMPLETED_JOB_NAME + " as sink into m select v, v from t");
        Job job = instance().getJet().getJob(COMPLETED_JOB_NAME);
        assertNotNull(job);
        job.join();
    }

    private long countActiveJobs() {
        return instance().getJet().getJobs().stream().filter(j -> !j.getStatus().isTerminal()).count();
    }
}
