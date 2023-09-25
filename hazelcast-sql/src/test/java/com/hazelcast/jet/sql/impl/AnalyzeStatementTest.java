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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        final SqlPlanImpl.SelectPlan plan = assertQueryPlan("ANALYZE WITH OPTIONS('opt1'='opt1val', 'opt2'='opt2val') "
                + "SELECT * FROM test");
        assertTrue(plan.isAnalyzed());
        assertEquals("opt1val", plan.getAnalyzeOptions().get("opt1"));
        assertEquals("opt2val", plan.getAnalyzeOptions().get("opt2"));
    }
}
