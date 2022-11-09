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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
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
    }

    public JetJoinInfo joinInfo(QueryParameterMetadata parameterMetadata) {
        JoinInfo joinInfo = analyzeCondition();
        RexNode predicate = joinInfo.getRemaining(getCluster().getRexBuilder());
        Expression<Boolean> nonEquiCondition = filter(schema(parameterMetadata), predicate, parameterMetadata);

        Expression<Boolean> condition = filter(schema(parameterMetadata), getCondition(), parameterMetadata);

        return new JetJoinInfo(
                getJoinType(),
                joinInfo.leftKeys.toIntArray(),
                joinInfo.rightKeys.toIntArray(),
                nonEquiCondition,
                condition
        );
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

    public long minWindowSize() {
        assert !postponeTimeMap.isEmpty();
        return minWindowSize(postponeTimeMap);
    }

    // Separated due to testing concerns
    static long minWindowSize(Map<Integer, Map<Integer, Long>> postponeTimeMap) {
        long min = Long.MAX_VALUE;

        // postpone map representation is timeA -> timeB -> constant
        // it means, window size is map[timeA][timeB] + map[timeB][timeA]
        for (Map.Entry<Integer, Map<Integer, Long>> outerEntry : postponeTimeMap.entrySet()) {
            for (Map.Entry<Integer, Long> innerEntry : outerEntry.getValue().entrySet()) {
                long windowSize = Math.abs(innerEntry.getValue()) +
                        Math.abs(postponeTimeMap.get(innerEntry.getKey()).get(outerEntry.getKey()));
                min = Math.min(min, windowSize);
            }
        }
        return min;
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
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
