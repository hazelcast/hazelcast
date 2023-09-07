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

import com.hazelcast.config.Config;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AnalyzeStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        final Config config = smallInstanceConfig();
        config.getMetricsConfig().setEnabled(true);
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        initialize(1, null);
    }

    @Test
    public void test_selectStreaming() {
        final SqlResult result = instance().getSql().execute("ANALYZE SELECT * FROM TABLE(GENERATE_STREAM(10))");
        assertNotEquals(-1L, result.jobId());

        final Iterator<SqlRow> it = result.iterator();
        for (int i = 0; i < 5; i++) {
            it.next();
        }

        final Job job = instance().getJet().getJob(result.jobId());
        assertNotNull(job);
        final JobMetrics metrics = job.getMetrics();
        assertNotNull(metrics);
        assertNotNull(metrics.get("executionStartTime"));
        assertTrue(metrics.get("executionStartTime").get(0).value() > 0L);
    }

    @Test
    public void test_insertStreaming() {
        createMapping("test", Long.class, String.class);
        final SqlResult result = instance().getSql().execute("ANALYZE INSERT INTO test SELECT v, 'value#' || v "
                + "FROM TABLE(GENERATE_STREAM(10))");
        final Job job = instance().getJet().getJob(result.jobId());
        assertNotNull(job);

        assertTrueEventually(() -> assertFalse(instance().getMap("test").isEmpty()));
        final JobMetrics metrics = job.getMetrics();
        assertNotNull(metrics);
        assertNotNull(metrics.get("executionStartTime"));
        assertTrue(metrics.get("executionStartTime").get(0).value() > 0L);
    }
}
