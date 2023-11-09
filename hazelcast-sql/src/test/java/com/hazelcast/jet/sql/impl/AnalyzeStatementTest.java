/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Objects;

import static com.hazelcast.jet.sql.impl.SqlPlanImpl.SelectPlan;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AnalyzeStatementTest extends SqlEndToEndTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_options() {
        createMapping("test", Long.class, String.class);
        assertFalse(assertQueryPlan("SELECT * FROM test").isAnalyzed());
        assertTrue(assertQueryPlan("ANALYZE SELECT * FROM test").isAnalyzed());

        // Default options
        SelectPlan plan = assertQueryPlan(
                "ANALYZE WITH OPTIONS("
                        + "'processingGuarantee'='exactlyOnce', "
                        + "'snapshotIntervalMillis'='121', "
                        + "'initialSnapshotName'='pressF', "
                        + "'maxProcessorAccumulatedRecords'='100'"
                        + ") SELECT * FROM test");
        assertTrue(plan.isAnalyzed());

        assertFalse(plan.analyzeJobConfig().isSplitBrainProtectionEnabled());
        assertFalse(plan.analyzeJobConfig().isAutoScaling());
        assertFalse(plan.analyzeJobConfig().isSuspendOnFailure());

        assertTrue(plan.analyzeJobConfig().isMetricsEnabled());
        assertTrue(plan.analyzeJobConfig().isStoreMetricsAfterJobCompletion());

        assertEquals(ProcessingGuarantee.EXACTLY_ONCE, plan.analyzeJobConfig().getProcessingGuarantee());
        assertEquals(121L, plan.analyzeJobConfig().getSnapshotIntervalMillis());
        assertEquals("pressF", plan.analyzeJobConfig().getInitialSnapshotName());
        assertEquals(100, plan.analyzeJobConfig().getMaxProcessorAccumulatedRecords());
    }

    @Test
    public void test_overridableOptions() {
        createMapping("test", Long.class, String.class);
        SelectPlan plan = assertQueryPlan(
                "ANALYZE WITH OPTIONS("
                        + "'metricsEnabled'='false', "
                        + "'storeMetricsAfterJobCompletion'='false'"
                        + ") SELECT * FROM test");
        assertTrue(plan.isAnalyzed());
        assertFalse(plan.analyzeJobConfig().isMetricsEnabled());
        assertFalse(plan.analyzeJobConfig().isStoreMetricsAfterJobCompletion());
    }

    @Test
    public void test_useUnsupportedOptionFails() {
        createMapping("test", Long.class, String.class);
        String expectedErrorDescription = "Job option is not supported for ANALYZE";
        assertThatThrownBy(() -> sqlService.execute(
                "ANALYZE WITH OPTIONS('splitBrainProtectionEnabled'='true') SELECT * FROM test"))
                .hasMessageContaining(expectedErrorDescription);

        assertThatThrownBy(() -> sqlService.execute(
                "ANALYZE WITH OPTIONS('autoScaling'='true') SELECT * FROM test"))
                .hasMessageContaining(expectedErrorDescription);

        assertThatThrownBy(() -> sqlService.execute(
                "ANALYZE WITH OPTIONS('suspendOnFailure'='true') SELECT * FROM test"))
                .hasMessageContaining(expectedErrorDescription);
    }

    @Test
    public void test_select() {
        createMapping("test", Long.class, String.class);
        instance().getSql().execute("INSERT INTO test VALUES (1, 'testVal')");
        assertRowsAnyOrder("ANALYZE SELECT * FROM test WHERE TRUE", rows(2, 1L, "testVal"));
        final Job job = instance().getJet().getJobs()
                .stream()
                .filter(j -> Objects.equals(
                        j.getConfig().getArgument("__sql.queryText"),
                        "ANALYZE SELECT * FROM test WHERE TRUE"
                ))
                .findFirst()
                .orElse(null);
        assertNotNull(job);
        assertFalse(job.isLightJob());
    }

    @Test
    public void test_insert() {
        createMapping("test", Long.class, Long.class);

        final String insertQuery = "INSERT INTO test SELECT v, v from table(generate_series(1,2))";
        assertJobIsAnalyzed(insertQuery);
        assertEquals(2, instance().getMap("test").size());
    }

    @Test
    public void test_update() {
        createMapping("test", Long.class, Long.class);
        instance().getMap("test").put(1L, 1L);

        final String updateQuery = "UPDATE test SET this = 3 WHERE this = 1 AND this IS NOT NULL";
        assertJobIsAnalyzed(updateQuery);
        assertEquals(3L, instance().getMap("test").get(1L));
    }

    @Test
    public void test_delete() {
        createMapping("test", Long.class, Long.class);
        instance().getMap("test").put(1L, 1L);

        final String deleteQuery = "DELETE FROM test WHERE this = 1 AND this IS NOT NULL";
        assertJobIsAnalyzed(deleteQuery);
        assertTrue(instance().getMap("test").isEmpty());
    }

    @Test
    public void test_suspend_isForbidden() {
        String query = "SELECT v, v FROM TABLE(generate_stream(1000))";
        instance().getSql().execute("ANALYZE " + query);
        Job job = instance().getJet().getJobs()
                .stream()
                .filter(j -> Objects.equals(
                        j.getConfig().getArgument("__sql.queryText"),
                        "ANALYZE " + query
                ))
                .findFirst()
                .orElse(null);

        assertNotNull(job);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.suspend();
        assertJobStatusEventually(job, JobStatus.FAILED);
    }

    @Test
    public void test_restart_isForbidden() {
        String query = "SELECT v, v FROM TABLE(generate_stream(1000))";
        instance().getSql().execute("ANALYZE " + query);
        Job job = instance().getJet().getJobs()
                .stream()
                .filter(j -> Objects.equals(
                        j.getConfig().getArgument("__sql.queryText"),
                        "ANALYZE " + query
                ))
                .findFirst()
                .orElse(null);

        assertNotNull(job);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        job.restart();
        assertJobStatusEventually(job, JobStatus.FAILED);
    }

    private static void assertJobIsAnalyzed(String query) {
        instance().getSql().execute("ANALYZE " + query);
        Job job = instance().getJet().getJobs()
                .stream()
                .filter(j -> Objects.equals(
                        j.getConfig().getArgument("__sql.queryText"),
                        "ANALYZE " + query
                ))
                .findFirst()
                .orElse(null);
        assertNotNull(job);
        assertFalse(job.isLightJob());
    }
}
