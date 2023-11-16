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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Objects;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

/**
 * Note: We prepared a separate test suite to prevent tests to be flaky
 *  because {@link AnalyzeStatementTest} inherits {@link SimpleTestInClusterSupport},
 *  where cluster members are shared between tests. Finding job by
 *  the SQL query is not unique because old/parallel jobs are also visible.
 *  We do not want to share the cluster between the tests.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlAnalyzedJobSuspensionIsForbiddenTest extends JetTestSupport {
    private HazelcastInstance instance;

    @Before
    public void setUp() throws Exception {
        instance = createHazelcastInstance();
    }

    @Test
    public void test_suspendJob() {
        // Given
        Job job = runQuery();

        // When
        job.suspend();

        // Then
        assertThatThrownBy(job::join)
                .isInstanceOf(CancellationException.class);
        // Note: this exception doesn't have message.
    }

    @Test
    public void test_restartJob() {
        // Given
        Job job = runQuery();

        // When
        job.restart();

        // Then
        assertThatThrownBy(job::join)
                .isInstanceOf(CancellationException.class);
        // Note: this exception doesn't have message.
    }

    @Test
    public void test_changeClusterStateToPassive() {
        // When
        Job job = runQuery();

        // Then
        instance.getCluster().changeClusterState(ClusterState.PASSIVE);
        instance.getCluster().changeClusterState(ClusterState.ACTIVE);

        assertThatThrownBy(job::join)
                .isInstanceOf(CancellationException.class);
    }

    private Job runQuery() {
        // Given
        String query = "SELECT v, v FROM TABLE(generate_stream(1))";
        instance.getSql().execute("ANALYZE " + query);

        // When
        Job job = instance.getJet().getJobs()
                .stream()
                .filter(j -> Objects.equals(
                        j.getConfig().getArgument("__sql.queryText"),
                        "ANALYZE " + query
                ))
                .findFirst()
                .orElse(null);

        // Then
        assertNotNull(job);
        assertJobStatusEventually(job, JobStatus.RUNNING);
        return job;
    }
}
