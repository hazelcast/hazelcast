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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertNotNull;

// Tests for https://github.com/hazelcast/hazelcast/issues/23403.

/*
 Note: we only have a test for SINK and INSERT, but not for DELETE or UPDATE, even
 though the CreateTopLevelDagVisitor contains code to calculate the throttling interval
 for those. The reason is that we currently only support INSERT and SINK in CREATE
 JOB command, and a streaming DML must use CREATE JOB.
 */
public class SqlAggregationWithDmlTest extends SqlTestSupport {

    private static KafkaTestSupport kafkaTestSupport;
    private static SqlService sql;

    @BeforeClass
    public static void beforeClass() throws Exception {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        kafkaTestSupport.createTopic("foo_topic", 1);

        initialize(2, null);
        sql = instance().getSql();
    }

    @Before
    public void before() {
        createMapping("foo_map", Long.class, Long.class);
        sql.execute("CREATE MAPPING foo_topic(\n" +
                "    tick BIGINT,\n" +
                "    ticker VARCHAR,\n" +
                "    price INT)\n" +
                "TYPE Kafka\n" +
                "OPTIONS (\n" +
                "    'keyFormat' = 'int',\n" +
                "    'valueFormat' = 'json-flat',\n" +
                "    'bootstrap.servers' = '" + kafkaTestSupport.getBrokerConnectionString() + "')");
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test_sink() {
        test_sink_insert("SINK");
    }

    @Test
    public void test_insert() {
        test_sink_insert("INSERT");
    }

    private void test_sink_insert(String command) {
        sql.execute("CREATE JOB jobAVG AS " +
                command + " INTO foo_map" +
                " SELECT window_start, window_end FROM " +
                "TABLE(TUMBLE(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE foo_topic, DESCRIPTOR(tick), 2)))" +
                "  , DESCRIPTOR(tick)" +
                " ,10" +
                ")) " +
                "GROUP BY window_start, window_end");

        Job job = instance().getJet().getJob("jobAVG");
        assertNotNull(job);
        assertJobStatusEventually(job, RUNNING);
    }
}
