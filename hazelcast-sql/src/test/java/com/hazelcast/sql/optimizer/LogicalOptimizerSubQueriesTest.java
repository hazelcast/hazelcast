/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.optimizer;

import com.hazelcast.sql.impl.calcite.opt.logical.AggregateLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.JoinLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.optimizer.support.LogicalOptimizerTestSupport;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.Table;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;

// TODO: Tests with SINGLE_VALUE decorrelations (i.e. when the subquery is in the SELECT or WHERE, etc)
public class LogicalOptimizerSubQueriesTest extends LogicalOptimizerTestSupport {
    @Override
    protected HazelcastSchema createDefaultSchema() {
        Map<String, Table> tableMap = new HashMap<>();

        tableMap.put("r", partitionedTable("r", fields("r", INT, "r1", INT, "r2", INT, "r3", INT), null, 100));
        tableMap.put("s", partitionedTable("s", fields("s", INT, "s1", INT, "s2", INT, "s3", INT), null, 100));

        return new HazelcastSchema(tableMap);
    }

    /**
     * Test correlated query with IN. In general case it is organized as an inner join on top of parent (left) relation with
     * the deduplicated bottom (right) relation. Here we the join condition is basically (r1=s1 and r2=s2). But the given pair
     * of (r1, r2) may have several matches of (s1, s2), hence the deduplication of S.
     */
    @Test
    public void testInCorrelated() {
        RelNode rootNode = optimizeLogical(
            "SELECT r.r3 FROM r WHERE r.r1 IN (SELECT s.s1 FROM s WHERE s.s2 = r.r2)"
        );

        ProjectLogicalRel project = assertProject(
            rootNode,
            list(column(2))
        );

        JoinLogicalRel join = assertJoin(
            project.getInput(),
            JoinRelType.INNER,
            and(
                compareColumnsEquals(1, 4), // r2=s2
                compareColumnsEquals(0, 3)  // r1=s1
            )
        );

        assertScan(
            join.getLeft(),
            "r",
            list(1, 2, 3),
            null
        );

        AggregateLogicalRel rightAgg = assertAggregate(
            join.getRight(),
            list(0, 1),
            list()
        );

        assertScan(
            rightAgg.getInput(),
            "s",
            list(1, 2),
            IsNotNullPredicate.create(column(2))
        );
    }

    /**
     * Test non-correlated query with IN. It should be optimized to a simple semi-join.
     */
    @Test
    public void testInNotCorrelated() {
        RelNode rootNode = optimizeLogical(
            "SELECT r.r2 FROM r WHERE r.r1 IN (SELECT s.s1 FROM s WHERE s.s2 < 50)"
        );

        ProjectLogicalRel project = assertProject(
            rootNode,
            list(column(1))
        );

        JoinLogicalRel join = assertJoin(
            project.getInput(),
            JoinRelType.SEMI,
            compareColumnsEquals(0, 2)
        );

        assertScan(
            join.getLeft(),
            "r",
            list(1, 2),
            null
        );

        assertScan(
            join.getRight(),
            "s",
            list(1),
            compare(column(2), constant(50), ComparisonMode.LESS_THAN)
        );

        System.out.println(RelOptUtil.toString(rootNode));
    }

    @Test
    public void testNotInCorrelated() {
        RelNode rel = optimizeLogical(
            "SELECT r.r FROM r WHERE r.r NOT IN (SELECT s.s2 FROM s WHERE s.s1 = r.r)"
        );

        // TODO: Good, but very complex

        System.out.println(RelOptUtil.toString(rel));
    }

    @Test
    public void testNotInNotCorrelated() {
        RelNode rel = optimizeLogical(
            "SELECT r.r FROM r WHERE r.r NOT IN (SELECT s.s FROM s)"
        );

        // TODO: Good, but very complex

        System.out.println(RelOptUtil.toString(rel));
    }

    @Test
    public void testExistsCorrelated() {
        RelNode rootNode = optimizeLogical(
            "SELECT r.r1 FROM r WHERE EXISTS (SELECT * FROM s WHERE s.s1 = r.r2)"
        );

        // TODO: Optimized properly into semijoin. Finalize test.

        System.out.println(RelOptUtil.toString(rootNode));
    }

    @Test
    public void testExistsNotCorrelated() {
        RelNode rel = optimizeLogical(
            "SELECT r.r FROM r WHERE EXISTS (SELECT s.s1 FROM s WHERE s.s2 > 50)"
        );

        // TODO: Optimized properly from the Calcite perspective. The right not correlated part is created in a form of
        //  LogicalAggregate <- LogicalProject(true) <- LogicalTableScan[filter]. It always produces either zero or one row.
        //  Need to think how to optimize it properly. One strategy could be to collect results from all the sites in a form
        //  of a single value - either 0 or 1 - and then broadcast it to between the nodes, thus forming REPLICATED
        //  distribution. The good question is where to perform this optimization - on logical phase, or during join planning?
        //  This is a good example where seemingly dangerous broadcast indeed becomes the most adequate strategy.

        // TODO: But why testInNotCorrelated produces better results then? Perhaps because of different NULL handling for IN and EXISTS?

        // TODO: Finish test for now.

        System.out.println(RelOptUtil.toString(rel));
    }

    // TODO: Make sure that the right stops as soon as the very first entry is found. It should be possible at least for
    //  non-correlated subqueries. No need to aggregate the whole set.
    @Test
    public void testNotExistsCorrelated() {
        RelNode rel = optimizeLogical(
            "SELECT r.r FROM r WHERE NOT EXISTS (SELECT * FROM s WHERE s.s = r.r)"
        );

        // TODO: Looks good! Write test

        System.out.println(RelOptUtil.toString(rel));
    }

    @Test
    public void testNotExistsNotCorrelated() {
        RelNode rel = optimizeLogical(
            "SELECT r.r FROM r WHERE NOT EXISTS (SELECT s.s FROM s)"
        );

        // TODO: Implemented in the same way as correlated NOT EXISTS: dedup S, then do r LEFT OUTER JOIN s. The problem is that
        //  this is a cross join! Are there any better strategies to handle this?

        System.out.println(RelOptUtil.toString(rel));
    }
}
