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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AnalyzeStatementTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void test_select() {
        createMapping("test", Long.class, String.class);
        instance().getSql().execute("INSERT INTO test VALUES (1, 'testVal')");

        final SqlResult result = instance().getSql().execute("ANALYZE SELECT * FROM test");
        final SqlRow row = result.iterator().next();
        assertNotNull(row);
        assertEquals(1L, (long) row.getObject("__key"));
        assertEquals("testVal", row.getObject("this"));
        result.close();

        final Job job = instance().getJet().getJob(result.jobId());
        assertNotNull(job);
    }

    @Test
    public void test_selectStreaming() {
        final SqlResult result = instance().getSql().execute("ANALYZE SELECT * FROM TABLE(GENERATE_STREAM(1))");
        assertNotEquals(-1L, result.jobId());
        final Job job = instance().getJet().getJob(result.jobId());
        assertNotNull(job);
    }
}
