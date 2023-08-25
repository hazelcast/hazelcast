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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.metadataQuery;

public class StreamToStreamJoinPhysicalRel extends JoinPhysicalRel {
    // Same postponeTimeMap as described in the TDD, but uses field indexes instead of WM keys,
    // which are not assigned yet
    private final Map<Integer, Map<Integer, Long>> postponeTimeMap;

    @SuppressWarnings("checkstyle:ParameterNumber")
    StreamToStreamJoinPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            Map<Integer, Map<Integer, Long>> postponeTimeMap
    ) {
        super(cluster, traitSet, left, right, condition, joinType);

        this.postponeTimeMap = postponeTimeMap;
        assert !postponeTimeMap.isEmpty();
    }

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors() {
        return timeExtractors(getLeft());
    }

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors() {
        return timeExtractors(getRight());
    }

    private Map<Integer, ToLongFunctionEx<JetSqlRow>> timeExtractors(RelNode input) {
        Map<Integer, ToLongFunctionEx<JetSqlRow>> timeExtractors = new HashMap<>();
        for (Integer i : metadataQuery(this).extractWatermarkedFields(input).getFieldIndexes()) {
            timeExtractors.put(i, row -> WindowUtils.extractMillis(row.getRow().get(i)));
        }
        return timeExtractors;
    }

    public Map<Integer, Map<Integer, Long>> postponeTimeMap() {
        return postponeTimeMap;
    }

    public long minimumSpread() {
        return minimumSpread(postponeTimeMap, left.getRowType().getFieldCount());
    }

    // Separated due to testing concerns
    static long minimumSpread(Map<Integer, Map<Integer, Long>> postponeTimeMap, int leftColumns) {
        long[] min = {Long.MAX_VALUE, Long.MAX_VALUE};

        // Postpone map representation is timeA -> timeB -> constant, which represents timeA > timeB - constant
        // We collect the minimums of the lower and upper bounds into min[0] and min[1].
        // The spread is then the distance between the lower and upper bounds.
        for (Map.Entry<Integer, Map<Integer, Long>> outerEntry : postponeTimeMap.entrySet()) {
            for (Map.Entry<Integer, Long> innerEntry : outerEntry.getValue().entrySet()) {
                int side = outerEntry.getKey() < leftColumns ? 0 : 1;
                min[side] = Math.min(min[side], innerEntry.getValue());
            }
        }
        assert min[0] < Long.MAX_VALUE && min[1] < Long.MAX_VALUE;
        // The spread should be a distance, so it suggests a subtraction. But we're adding instead, because the
        // upper bound is stored as inverted lower bound.
        long spread = min[0] + min[1];
        // the result can be negative or, make it 1 at least
        return Math.max(spread, 1);
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onStreamToStreamJoin(this);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone
    ) {
        return new StreamToStreamJoinPhysicalRel(
                getCluster(),
                traitSet,
                left,
                right,
                getCondition(),
                joinType,
                postponeTimeMap
        );
    }
}
