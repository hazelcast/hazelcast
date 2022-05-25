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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTableStatistic;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.TablesStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class CalcDropLateItemsTransposeTest extends OptimizerTestSupport {
    private NodeEngine nodeEngine;
    private TableResolver resolver;
    private SqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        nodeEngine = getNodeEngine(instance());
        resolver = new TableResolverImpl(nodeEngine, new TablesStorage(nodeEngine), new SqlConnectorCache(nodeEngine));
        sqlService = instance().getSql();

        String stream = "stream1";
        TestStreamSqlConnector.create(
                sqlService,
                stream,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP),
                row(1, timestamp(1L))
        );
    }

    @Test
    public void given_calcAndDropItemsRelTransposes_whenNoProjectPermutes_then_success() {
        // TODO: optimizer support for stream tables and assertPlan(..)
        String sql = "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(b), INTERVAL '0.001' SECOND))";

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));
        HazelcastTable streamingTable = streamingTable(resolver.getTables().get(0), 1L);

        RelNode node = preOptimize(sql, streamingTable);
        assertPlan(
                node,
                plan(
                        planRow(0, LogicalTableFunctionScan.class),
                        planRow(1, LogicalTableScan.class)
                )
        );

        LogicalRel logicalRel = optimizeLogical(sql, streamingTable);
        assertPlan(
                logicalRel,
                plan(
                        planRow(0, DropLateItemsLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );

        assertRowsEventuallyInAnyOrder(sql, ImmutableList.of(new Row(1, timestamp(1L))));
    }

    @Test
    public void given_calcAndDropItemsRelTransposes_whenProjectPermutes_then_success() {
        // TODO: optimizer support for stream tables and assertPlan(..)
        String sql = "SELECT b, a FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(b), INTERVAL '0.001' SECOND))";

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));
        HazelcastTable streamingTable = streamingTable(resolver.getTables().get(0), 1L);

        RelNode node = preOptimize(sql, streamingTable);
        assertPlan(
                node,
                plan(
                        planRow(0, LogicalCalc.class),
                        planRow(1, LogicalTableFunctionScan.class),
                        planRow(2, LogicalTableScan.class)
                )
        );

        LogicalRel logicalRel = optimizeLogical(sql, streamingTable);
        /* Desired behaviour of logical optimizations:
          1. Initial state
          - Calc
          -- DropRel
          --- FullScan

          2. DropRel and Calc transposition
          - DropRel
          -- Calc
          --- FullScan

          3. Calc push-down into FullScan
          - DropRel
          -- FullScan
         */
        assertPlan(
                logicalRel,
                plan(
                        planRow(0, DropLateItemsLogicalRel.class),
                        planRow(1, FullScanLogicalRel.class)
                )
        );
        assertRowsEventuallyInAnyOrder(sql, singletonList(new Row(timestamp(1L), 1)));
    }

    private static HazelcastTable streamingTable(Table table, long rowCount) {
        return new HazelcastTable(table, new HazelcastTableStatistic(rowCount));
    }
}
