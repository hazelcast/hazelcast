/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TestProcessors;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class ShowStatementTest extends SqlTestSupport {

    private final SqlService sqlService = instance().getSql();

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Test
    public void test_showStatement_metadata() {
        SqlRowMetadata expectedMetadata =
                new SqlRowMetadata(singletonList(new SqlColumnMetadata("name", SqlColumnType.VARCHAR, false)));

        assertThat(sqlService.execute("show mappings").getRowMetadata()).isEqualTo(expectedMetadata);
        assertThat(sqlService.execute("show jobs").getRowMetadata()).isEqualTo(expectedMetadata);
    }

    @Test
    public void when_showMapping_empty() {
        assertRowsOrdered("show mappings", emptyList());
    }

    @Test
    public void test_showMapping() {
        List<String> mappingNames = IntStream.range(0, 5).mapToObj(i -> "t" + i).collect(toList());
        for (String mappingName : mappingNames) {
            TestBatchSqlConnector.create(sqlService, mappingName, 1);
        }

        assertRowsOrdered("show mappings", Util.toList(mappingNames, Row::new));
    }

    @Test
    public void test_withOptionalExternalKeyword() {
        TestBatchSqlConnector.create(sqlService, "t", 1);
        assertRowsOrdered("show external mappings", singletonList(new Row("t")));
    }

    @Test
    public void when_implicitMapping_then_notVisible() {
        IMap<Integer, Integer> myMap = instance().getMap("my_map");
        for (int i = 0; i < 10; i++) {
            myMap.put(i, i);
        }
        assertRowsOrdered("show mappings", emptyList());
    }

    @Test
    public void test_showJobsEmpty() {
        assertRowsOrdered("show jobs", emptyList());
    }

    @Test
    public void test_showJobs() {
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));
        sqlService.execute("create job testJob as " +
                "sink into m " +
                "select v, v from table(generate_stream(1))");
        assertRowsOrdered("show jobs", singletonList(new Row("testJob")));
    }

    @Test
    public void when_jobCompleted_then_notShown() {
        TestBatchSqlConnector.create(sqlService, "t", 1);
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));
        sqlService.execute("create job testJob as " +
                "sink into m " +
                "select v, v from t");
        assertTrueEventually(() -> assertRowsOrdered("show jobs", emptyList()));
    }

    @Test
    public void when_jobSubmittedThroughJava_then_shown() {
        createJobInJava("testJob");
        assertRowsOrdered("show jobs", singletonList(new Row("testJob")));
    }

    @Test
    public void when_unnamedJob_then_notListed() {
        createJobInJava(null);
        assertRowsOrdered("show jobs", emptyList());
    }

    private void createJobInJava(String jobName) {
        DAG dag = new DAG();
        dag.newVertex("v", () -> new TestProcessors.MockP().streaming());
        instance().newJob(dag, new JobConfig().setName(jobName));
    }
}
