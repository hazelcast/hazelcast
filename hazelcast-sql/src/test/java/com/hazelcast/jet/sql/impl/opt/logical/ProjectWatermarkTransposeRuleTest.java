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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProjectWatermarkTransposeRuleTest extends SqlTestSupport {
    private SqlService sqlService;
    private KafkaTestSupport kafkaTestSupport;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        sqlService = instance().getSql();
        kafkaTestSupport = new KafkaTestSupport();
    }

    @Test
    public void test() {
        sqlService.execute("CREATE MAPPING trades TYPE Kafka OPTIONS ("
                + "  'keyFormat'='varchar'"
                + ", 'valueFormat'='json'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ")");
        sqlService.execute("CREATE VIEW IF NOT EXISTS v AS " +
                "SELECT JSON_VALUE(this, '$.timestamp' RETURNING BIGINT) AS ts FROM trades");

        final String sql = "SELECT window_start, window_end, SUM(ts) FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE(v), DESCRIPTOR(ts), 2))), " +
                "DESCRIPTOR(ts), 4, 2)) " +
                "GROUP BY window_start, window_end";


        assertThat(sqlService.execute(sql)).isNotNull();

//        HazelcastTable table = partitionedTable("map", asList(field(KEY, INT), field(VALUE, INT)), 1);
//
//        final String sql = "SELECT * FROM " +
//                "TABLE(HOP(" +
//                "  (SELECT * FROM TABLE(IMPOSE_ORDER((SELECT __key, this FROM map), DESCRIPTOR(__key), 2))), " +
//                "DESCRIPTOR(__key), 4, 2)) " +
//                "GROUP BY window_start, window_end, __key, this";
//
//        assertPlan(optimizeLogical(sql, table), plan(
//                planRow(0, ProjectLogicalRel.class),
//                planRow(1, AggregateLogicalRel.class),
//                planRow(2, ProjectLogicalRel.class),
//                planRow(3, SlidingWindowLogicalRel.class),
//                planRow(4, FullScanLogicalRel.class)
//        ));
    }
}
