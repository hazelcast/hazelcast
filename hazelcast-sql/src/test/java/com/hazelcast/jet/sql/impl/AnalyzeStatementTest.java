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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Objects;

import static com.hazelcast.jet.sql.impl.SqlPlanImpl.SelectPlan;
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
    public void test_select() {
        createMapping("test", Long.class, String.class);
        assertFalse(assertQueryPlan("SELECT * FROM test").isAnalyzed());
        assertTrue(assertQueryPlan("ANALYZE SELECT * FROM test").isAnalyzed());
        SelectPlan plan = assertQueryPlan(
                "ANALYZE WITH OPTIONS('opt1'='opt1val', 'opt2'='opt2val') SELECT * FROM test");
        assertTrue(plan.isAnalyzed());
        assertEquals("opt1val", plan.getAnalyzeOptions().get("opt1"));
        assertEquals("opt2val", plan.getAnalyzeOptions().get("opt2"));

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

    // TODO: test other DMLs when we have proper OPTIONS support
    @Test
    public void test_insert() {
        createMapping("test", Long.class, Long.class);
        final String baseQuery = "INSERT INTO test SELECT v, v from table(generate_series(1,2))";
        assertFalse(assertDmlQueryPlan(baseQuery).isAnalyzed());
        assertTrue(assertDmlQueryPlan("ANALYZE " + baseQuery).isAnalyzed());
        SqlPlanImpl.DmlPlan plan = assertDmlQueryPlan(
                "ANALYZE WITH OPTIONS('opt1'='opt1val', 'opt2'='opt2val') " + baseQuery);
        assertTrue(plan.isAnalyzed());
        assertEquals("opt1val", plan.getAnalyzeOptions().get("opt1"));
        assertEquals("opt2val", plan.getAnalyzeOptions().get("opt2"));

        instance().getSql().execute("ANALYZE " + baseQuery);
        final Job job = instance().getJet().getJobs()
                .stream()
                .filter(j -> Objects.equals(
                        j.getConfig().getArgument("__sql.queryText"),
                        "ANALYZE " + baseQuery
                ))
                .findFirst()
                .orElse(null);
        assertNotNull(job);
        assertFalse(job.isLightJob());
    }
}
