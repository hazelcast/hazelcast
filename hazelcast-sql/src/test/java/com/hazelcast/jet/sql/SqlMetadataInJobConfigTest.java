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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_SQL_QUERY_TEXT;
import static com.hazelcast.jet.config.JobConfigArguments.KEY_SQL_UNBOUNDED;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertEquals;

@Ignore("https://github.com/hazelcast/hazelcast/issues/20372")
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlMetadataInJobConfigTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_selectMetadata_member() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult ignored = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            List<Job> runningJobs = getJobsByStatus(RUNNING);
            assertEquals(1, runningJobs.size());
            JobConfig config = runningJobs.get(0).getConfig();
            assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
            assertEquals(Boolean.TRUE, config.getArgument(KEY_SQL_UNBOUNDED));
        }
    }

    @Test
    public void test_selectMetadata_client() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult ignored = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            List<Job> runningJobs = getJobsByStatus(RUNNING);
            assertEquals(1, runningJobs.size());
            JobConfig config = runningJobs.get(0).getConfig();
            assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
            assertEquals(Boolean.TRUE, config.getArgument(KEY_SQL_UNBOUNDED));
        }
    }

    @Test
    public void test_selectMetadata_clientJobSummary() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult ignored = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            List<JobSummary> jobSummaries = ((JetClientInstanceImpl) client().getJet()).getJobSummaryList().stream()
                    .filter(jobSummary -> jobSummary.getStatus() == RUNNING)
                    .collect(Collectors.toList());
            assertEquals(1, jobSummaries.size());

            JobSummary jobSummary = jobSummaries.get(0);
// TODO uncomment this when doing https://github.com/hazelcast/hazelcast/issues/20372
//            assertNotNull(jobSummary.getSqlSummary());
//            assertEquals(sql, jobSummary.getSqlSummary().getQuery());
//            assertEquals(Boolean.TRUE, jobSummary.getSqlSummary().isUnbounded());
        }
    }

    @Test
    public void test_createJobMetadata() {
        TestBatchSqlConnector.create(instance().getSql(), "src", 3);
        createMapping("dest", Integer.class, String.class);

        String sql = "CREATE JOB testJob AS INSERT INTO dest SELECT v * 2, 'value-' || v FROM src WHERE v < 2";
        instance().getSql().execute(sql);
        assertJobStatusEventually(instance().getJet().getJob("testJob"), COMPLETED);

        List<Job> completedJobs = getJobsByStatus(COMPLETED);
        assertEquals(1, completedJobs.size());
        JobConfig config = completedJobs.get(0).getConfig();
        assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
        assertEquals(Boolean.FALSE, config.getArgument(KEY_SQL_UNBOUNDED));
    }

    @Test
    public void test_dmlMetadata() {
        createMapping("dest", Integer.class, Integer.class);
        TestBatchSqlConnector.create(instance().getSql(), "src", 1, true);

        String sql = "INSERT INTO dest SELECT v, v FROM src";
        Future<SqlResult> f = spawn(() ->
                instance().getSql().execute(sql));
        awaitSingleRunningJob(instance());

        List<Job> runningJobs = getJobsByStatus(RUNNING);
        assertEquals(1, runningJobs.size());
        JobConfig config = runningJobs.get(0).getConfig();
        assertEquals(sql, config.getArgument(KEY_SQL_QUERY_TEXT));
        assertEquals(Boolean.FALSE, config.getArgument(KEY_SQL_UNBOUNDED));
    }

    private List<Job> getJobsByStatus(JobStatus status) {
        return instance().getJet().getJobs().stream()
                .filter(job -> job.getStatus() == status)
                .collect(Collectors.toList());
    }
}
