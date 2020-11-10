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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.sql.SqlTestSupport.javaSerializableMapDdl;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SqlJobManagementTest extends SimpleTestInClusterSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void when_streamingDmlWithoutCreateJob_then_fail() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        assertThatThrownBy(() -> sqlService.execute("SINK INTO dest SELECT v, v FROM src"))
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
        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE JOB fooJob AS INSERT INTO t1 SELECT FROM t2"))
                .hasMessageContaining("OR REPLACE is not supported for CREATE JOB");
    }

    @Test
    public void when_createJobUnknownOption_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS (badOption 'value') AS "
                        + "INSERT INTO t1 VALUES(1)"))
                .hasMessage("From line 1, column 25 to line 1, column 33: Unknown job option: badOption");
    }

    @Test
    public void when_snapshotIntervalNotNumber_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS (snapshotIntervalMillis 'foo') AS "
                        + "INSERT INTO t1 VALUES(1)"))
               .hasMessage("From line 1, column 48 to line 1, column 52: Invalid number for snapshotIntervalMillis: foo");
    }

    @Test
    public void when_badProcessingGuarantee_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE JOB foo OPTIONS (processingGuarantee 'foo') AS "
                        + "INSERT INTO t1 VALUES(1)"))
               .hasMessage("From line 1, column 45 to line 1, column 49: Unsupported value for processingGuarantee: foo");
    }

    @Test
    public void testJobSubmitAndCancel() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");

        assertNotNull("job doesn't exist", instance().getJob("testJob"));

        sqlService.execute("DROP JOB testJob");
    }

    @Test
    public void when_duplicateName_then_fails() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");

        assertThatThrownBy(() ->
                sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src"))
                .hasMessageContaining("Another active job with equal name (testJob) exists");
    }

    @Test
    public void when_duplicateName_and_ifNotExists_then_secondSubmissionIgnored() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");
        assertEquals(1, countActiveJobs());

        sqlService.execute("CREATE JOB IF NOT EXISTS testJob AS SINK INTO dest SELECT v, v FROM src");
        assertEquals(1, countActiveJobs());
    }

    @Test
    public void when_dropNonExistingJob_then_fail() {
        assertThatThrownBy(() ->
                sqlService.execute("DROP JOB nonExistingJob"))
                .hasMessageContaining("Job doesn't exist or already terminated");
    }

    @Test
    public void when_dropNonExistingJob_and_ifExists_then_ignore() {
        sqlService.execute("DROP JOB IF EXISTS nonExistingJob");
    }

    @Test
    public void test_jobOptions() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob " +
                "OPTIONS (" +
                // we use non-default value for each config option
                "processingGuarantee 'exactlyOnce'," +
                "snapshotIntervalMillis '6000'," +
                "autoScaling 'false'," +
                "splitBrainProtectionEnabled 'true'," +
                "metricsEnabled 'false'," +
                "initialSnapshotName 'fooSnapshot'," +
                "storeMetricsAfterJobCompletion 'true')" +
                "AS SINK INTO dest SELECT v, v FROM src");

        JobConfig config = instance().getJob("testJob").getConfig();

        assertEquals(EXACTLY_ONCE, config.getProcessingGuarantee());
        assertEquals(6000, config.getSnapshotIntervalMillis());
        assertFalse("isAutoScaling", config.isAutoScaling());
        assertTrue("isSplitBrainProtectionEnabled", config.isSplitBrainProtectionEnabled());
        assertFalse("isMetricsEnabled", config.isMetricsEnabled());
        assertEquals("fooSnapshot", config.getInitialSnapshotName());
    }

    @Test
    public void when_clientDisconnects_then_jobContinues() {
        JetInstance client = factory().newClient();
        SqlService sqlService = client.getSql();

        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");
        Job job = instance().getJob("testJob");
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
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");

        Job job = instance().getJob("testJob");
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
    public void when_snapshotExportWithoutOrReplace_then_orReplaceRequired() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");

        assertThatThrownBy(() -> sqlService.execute("CREATE SNAPSHOT mySnapshot FOR JOB testJob"))
                .hasMessageContaining("The OR REPLACE option is required for CREATE SNAPSHOT");
    }

    @Test
    public void when_snapshotExport_then_failNotEnterprise() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");

        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE SNAPSHOT mySnapshot FOR JOB testJob"))
                .hasMessageContaining("You need Hazelcast Jet Enterprise to use this feature");
    }

    @Test
    public void when_snapshotExport_jobDoesNotExist_then_fail() {
        assertThatThrownBy(() -> sqlService.execute("CREATE OR REPLACE SNAPSHOT mySnapshot FOR JOB nonExistentJob"))
                .hasMessageContaining("The job 'nonExistentJob' doesn't exist");
    }

    @Test
    public void when_dropJobWithSnapshot_then_failNotEnterprise() {
        sqlService.execute("CREATE MAPPING src TYPE TestStream");
        sqlService.execute(javaSerializableMapDdl("dest", Long.class, Long.class));

        sqlService.execute("CREATE JOB testJob AS SINK INTO dest SELECT v, v FROM src");

        assertThatThrownBy(() -> sqlService.execute("DROP JOB testJob WITH SNAPSHOT mySnapshot"))
                .hasMessageContaining("You need Hazelcast Jet Enterprise to use this feature");
    }

    private long countActiveJobs() {
        return instance().getJobs().stream().filter(j -> !j.getStatus().isTerminal()).count();
    }
}
