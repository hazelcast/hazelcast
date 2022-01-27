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
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JobSummary;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.sql.JobConfigAttributes.SQL_QUERY_KEY_NAME;
import static com.hazelcast.sql.JobConfigAttributes.SQL_UNBOUNDED_KEY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlQueryInJobConfigTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initializeWithClient(1, null, null);
    }

    @Test
    public void when_runningTheSelectQuery_then_queryAndUnboundedFlagCanBeFetchFromConfig() {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult result = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            List<Job> jobs = instance().getJet().getJobs();
            assertEquals(1, jobs.size());
            JobConfig config = jobs.get(0).getConfig();
            assertEquals(sql, config.getArgument(SQL_QUERY_KEY_NAME));
            assertEquals(Boolean.TRUE, config.getArgument(SQL_UNBOUNDED_KEY_NAME));
        }
    }

    @Test
    public void when_runningTheSelectQuery_then_queryAndUnboundedFlagCanBeFetchFromJobSummary() throws ExecutionException, InterruptedException {
        String sql = "SELECT * FROM table(generate_stream(1))";
        try (SqlResult result = client().getSql().execute(new SqlStatement(sql).setCursorBufferSize(1))) {
            List<JobSummary> jobSummaries = ((JetClientInstanceImpl) client().getJet()).getJobSummaryList();
            assertEquals(1, jobSummaries.size());

            JobSummary jobSummary = jobSummaries.get(0);
            assertNotNull(jobSummary.getSqlSummary());
            assertEquals(sql, jobSummary.getSqlSummary().getQuery());
            assertEquals(Boolean.TRUE, jobSummary.getSqlSummary().isUnbounded());
        }
    }

    @Test
    public void when_creatingJob_then_queryAndUnboundedFlagCanBeFetchFromConfig() {
        TestBatchSqlConnector.create(instance().getSql(), "src", 3);
        createMapping("dest", Integer.class, String.class);

        String sql = "CREATE JOB testJob AS INSERT INTO dest SELECT v * 2, 'value-' || v FROM src WHERE v < 2";
        instance().getSql().execute(sql);
        assertJobStatusEventually(instance().getJet().getJob("testJob"), COMPLETED);

        List<Job> jobs = instance().getJet().getJobs();
        assertEquals(1, jobs.size());
        JobConfig config = jobs.get(0).getConfig();
        assertEquals(sql, config.getArgument(SQL_QUERY_KEY_NAME));
        assertEquals(Boolean.FALSE, config.getArgument(SQL_UNBOUNDED_KEY_NAME));
    }

//    @Test
//    public void when_dml_then_queryAndUnboundedFlagCanBeFetchFromConfig() {
//        TestBatchSqlConnector.create(instance().getSql(), "src", 300);
//        createMapping("dest", Integer.class, String.class);
//
//        String sql = "INSERT INTO dest SELECT v * 2, 'value-' || v FROM src WHERE v < 200";
//        try (SqlResult execute = instance().getSql().execute(sql)) {
//            List<Job> jobs = instance().getJet().getJobs();
//            assertEquals(1, jobs.size());
//            JobConfig config = jobs.get(0).getConfig();
//            assertEquals(sql, config.getArgument(SQL_QUERY_KEY_NAME));
//            assertEquals(Boolean.FALSE, config.getArgument(SQL_UNBOUNDED_KEY_NAME));
//        }
//    }

}