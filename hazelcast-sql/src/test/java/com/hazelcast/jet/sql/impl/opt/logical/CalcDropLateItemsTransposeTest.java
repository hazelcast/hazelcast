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

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.TableResolver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class CalcDropLateItemsTransposeTest extends OptimizerTestSupport {
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
                asList("a", "b", "c"),
                asList(INTEGER, TIMESTAMP, VARCHAR),
                row(1, timestamp(1L), "1")
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

        assertRowsEventuallyInAnyOrder(sql, ImmutableList.of(new Row(1, timestamp(1L), "1")));
    }

    @Test
    public void given_calcAndDropItemsRelTransposes_whenProjectPermutes_then_success() {
        String sql = "SELECT c, a, b, c FROM TABLE(IMPOSE_ORDER(TABLE stream1, DESCRIPTOR(b), INTERVAL '0.001' SECOND))";

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

        HazelcastRelMetadataQuery query = HazelcastRelMetadataQuery.reuseOrCreate(RelMetadataQuery.instance());
        Set<Integer> wmIndexes = query.extractWatermarkedFields(logicalRel).getFieldIndexes();
        assertThat(wmIndexes.size()).isEqualTo(1);
        assertThat(wmIndexes.iterator().next()).isEqualTo(2);

        assertRowsEventuallyInAnyOrder(sql, singletonList(new Row("1", 1, timestamp(1L), "1")));
    }
}
