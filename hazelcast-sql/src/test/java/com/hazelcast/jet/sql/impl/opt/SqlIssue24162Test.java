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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.jet.sql.impl.opt.physical.CalcPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SlidingWindowAggregatePhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptTable;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Optimizer replace a right sub-tree in S2S Join with SWAgg as inputs,
 * in case, when both aggregation operations are the same :
 *  SELECT * FROM ( SELECT ... AVG(a) AS price FROM ... HOP(TABLE s1 ... ))
 *  JOIN          ( SELECT ... AVG(b) AS price FROM ... HOP(TABLE s2 ... )) ON ...
 * and after the physical optimization both scans reads the same stream :
 * <p>
 * (LEFT) FullScanPhysicalRel(table=[[hazelcast, public, **s1**[projects=[$0, $1]]]], ... discriminator=[1])
 * <p>
 * (RIGHT) FullScanPhysicalRel(table=[[hazelcast, public, **s1**[projects=[$0, $1]]]], ... discriminator=[2])
 * <p>
 * <a href="https://github.com/hazelcast/hazelcast/issues/24162">Issue link</a>
 */
public class SqlIssue24162Test extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test() {
        NodeEngine nodeEngine = getNodeEngine(instance());
        TableResolverImpl resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "s1";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                singletonList("a"),
                singletonList(BIGINT),
                row(1L)
        );

        String stream2 = "s2";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream2,
                singletonList("b"),
                singletonList(BIGINT),
                row(1L)
        );

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));
        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(1));

        HazelcastTable table = streamingTable(resolver.getTables().get(0), 1L);
        HazelcastTable table2 = streamingTable(resolver.getTables().get(1), 1L);

        String sql = "SELECT * FROM " +
                "( SELECT window_end, AVG(a) AS price1 FROM " +
                "    TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s1, DESCRIPTOR(a), 1))), DESCRIPTOR(a), 4, 1))" +
                "    GROUP BY window_end, a) s_1" +
                "  RIGHT JOIN " +
                "( SELECT window_end, AVG(b) AS price2 FROM " +
                "    TABLE(HOP((SELECT * FROM TABLE(IMPOSE_ORDER(TABLE s2, DESCRIPTOR(b), 1))), DESCRIPTOR(b), 4, 1))" +
                "    GROUP BY window_end, b) s_2" +
                " ON s_1.window_end = s_2.window_end";

        PhysicalRel optPhysicalRel = optimizePhysical(sql,
                singletonList(QueryDataType.BIGINT), table, table2).getPhysical();

        assertPlan(optPhysicalRel, plan(
                planRow(0, CalcPhysicalRel.class),
                planRow(1, StreamToStreamJoinPhysicalRel.class),
                planRow(2, SlidingWindowAggregatePhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, FullScanPhysicalRel.class),
                planRow(2, SlidingWindowAggregatePhysicalRel.class),
                planRow(3, CalcPhysicalRel.class),
                planRow(4, FullScanPhysicalRel.class)
        ));

        assertThat(OptUtils.isUnbounded(optPhysicalRel)).isTrue();
        FullScanPhysicalRel leftSource = (FullScanPhysicalRel) optPhysicalRel
                .getInput(0) // S2S
                .getInput(0) // SWA
                .getInput(0) // Calc
                .getInput(0);

        FullScanPhysicalRel rightSource = (FullScanPhysicalRel) optPhysicalRel
                .getInput(0) // S2S
                .getInput(1) // SWA
                .getInput(0) // Calc
                .getInput(0);

        assertThat(leftSource).isNotEqualTo(rightSource);
        RelOptTable leftTable = leftSource.getTable();
        RelOptTable rightTable = rightSource.getTable();
        assertThat(leftTable).isNotEqualTo(rightTable);
    }
}
