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

package com.hazelcast.jet.sql;

import com.hazelcast.config.Config;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.ResultLimitReachedException;
import com.hazelcast.test.ExceptionRecorder;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class LogExceptionSqlTest extends SimpleTestInClusterSupport {

    private static ExceptionRecorder recorder;

    @BeforeClass
    public static void setUpClass() {
        Config config = smallInstanceConfig();
        initialize(2, config);
        recorder = new ExceptionRecorder(instances(), Level.INFO);
    }

    @Before
    public void setUp() throws Exception {
        recorder.clear();
    }

    @Test
    public void when_limitInQueryReached_then_noExceptionIsThrown() {

        // when
        SqlService sql = instance().getSql();
        try (SqlResult result = sql.execute("select * from table(generate_stream(5)) limit 2")) {
            Iterator<SqlRow> rows = result.iterator();
            //noinspection WhileLoopReplaceableByForEach
            while (rows.hasNext()) {
                rows.next();
            }
        }

        // result is closed before the job is cleaned on member side, so wait here
        assertNoLightJobsLeftEventually(instance());

        // then
        List<Throwable> exceptions = recorder.exceptionsOfTypes(ResultLimitReachedException.class, JetException.class);
        assertThat(exceptions).isEmpty();
    }
}
