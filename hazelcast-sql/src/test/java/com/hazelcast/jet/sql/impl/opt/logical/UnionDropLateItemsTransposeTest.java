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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.TableResolver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static java.util.Arrays.asList;

public class UnionDropLateItemsTransposeTest extends OptimizerTestSupport {
    private TableResolver resolver;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        NodeEngine nodeEngine = getNodeEngine(instance());
        resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));

        String stream = "stream1";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP),
                row(0, timestamp(0L))
        );

        String stream2 = "stream2";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream2,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP),
                row(1, timestamp(1L))
        );

        String stream3 = "stream3";
        TestStreamSqlConnector.create(
                instance().getSql(),
                stream3,
                asList("x", "y"),
                asList(INTEGER, TIMESTAMP),
                row(2, timestamp(2L))
        );
    }

    @Ignore("This rule is disabled; introduce it and implement DropLateItemsP in correct way.")
    @Test
    public void when_unionAndDropItemsRelTransposes_then_transposes() {
        // TODO: optimizer support for stream tables and assertPlan(..)
        String sql = "" +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(b), INTERVAL '0.001' SECOND)) " +
                "UNION ALL " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream2, DESCRIPTOR(b), INTERVAL '0.001' SECOND))" +
                "UNION ALL " +
                "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE stream3, DESCRIPTOR(y), INTERVAL '0.001' SECOND))";

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));
        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(1));
        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(2));
        HazelcastTable streamingTable1 = streamingTable(resolver.getTables().get(0), 1L);
        HazelcastTable streamingTable2 = streamingTable(resolver.getTables().get(1), 1L);
        HazelcastTable streamingTable3 = streamingTable(resolver.getTables().get(2), 1L);

        RelNode node = preOptimize(sql, streamingTable1, streamingTable2, streamingTable3);
        assertPlan(
                node,
                plan(
                        planRow(0, LogicalUnion.class),
                        planRow(1, LogicalTableFunctionScan.class),
                        planRow(2, LogicalTableScan.class),
                        planRow(1, LogicalTableFunctionScan.class),
                        planRow(2, LogicalTableScan.class),
                        planRow(1, LogicalTableFunctionScan.class),
                        planRow(2, LogicalTableScan.class)
                )
        );

        LogicalRel logicalRel = optimizeLogical(sql, streamingTable1, streamingTable2, streamingTable3);
        assertPlan(
                logicalRel,
                plan(
                        planRow(0, DropLateItemsLogicalRel.class),
                        planRow(1, UnionLogicalRel.class),
                        planRow(2, FullScanLogicalRel.class),
                        planRow(2, FullScanLogicalRel.class),
                        planRow(2, FullScanLogicalRel.class)
                )
        );

        assertRowsEventuallyInAnyOrder(sql, asList(
                new Row(0, timestamp(0L)),
                new Row(1, timestamp(1L)),
                new Row(2, timestamp(2L))
        ));
    }
}
