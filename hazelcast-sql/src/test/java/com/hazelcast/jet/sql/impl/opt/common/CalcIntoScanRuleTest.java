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

package com.hazelcast.jet.sql.impl.opt.common;

import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.DropLateItemsLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static java.util.Collections.singletonList;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CalcIntoScanRuleTest extends OptimizerTestSupport {
    private SqlService sqlService;
    private TableResolverImpl resolver;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() throws Exception {
        sqlService = instance().getSql();

        NodeEngine nodeEngine = getNodeEngine(instance());
        resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));
    }

    @Test
    public void when_calcPushDownIntoScan_then_mergedFilterWereSimplified() {
        String name = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                name,
                singletonList("t"),
                singletonList(INTEGER),
                row(0),
                row(1),
                row(2)
        );

        String sql = "SELECT * FROM (" +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(t), 1)) WHERE t > 1 AND t = 2) " +
                "     WHERE t <= 2";

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));
        HazelcastTable streamingTable = streamingTable(resolver.getTables().get(0), 3L);

        assertPlan(
                preOptimize(sql, streamingTable),
                plan(
                        planRow(0, LogicalCalc.class),
                        planRow(1, LogicalTableFunctionScan.class),
                        planRow(2, LogicalTableScan.class)
                )
        );

        // Note: expected plan :
        // :- DropLateItemsPhysicalRel(traitSet=[PHYSICAL.[].ANY])
        //    :- FullScanPhysicalRel(... filter=AND(IS NOT NULL($0), =($0, 2))]]], ...)
        LogicalRel logicalRel = optimizeLogical(sql, streamingTable);
        assertPlan(
                logicalRel,
                plan(
                        planRow(0, DropLateItemsLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );

        FullScanLogicalRel scan = (FullScanLogicalRel) logicalRel.getInput(0);

        // here, we see that (t > 1 AND t = 2) AND t <= 2 was simplified to just t = 2
        assertContains(scan.toString(), "filter==($0, 2)]");
        assertTipOfStream(sql, singletonList(new Row(2)));
    }
}
