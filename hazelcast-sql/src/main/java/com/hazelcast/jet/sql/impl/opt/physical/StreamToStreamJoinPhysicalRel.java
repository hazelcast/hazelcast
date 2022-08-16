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
    protected StreamToStreamJoinPhysicalRel(
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
        RexNode predicate = analyzeCondition().getRemaining(getCluster().getRexBuilder());
        Expression<Boolean> nonEquiCondition = filter(schema(parameterMetadata), predicate, parameterMetadata);

        Expression<Boolean> condition = filter(schema(parameterMetadata), getCondition(), parameterMetadata);

        return new JetJoinInfo(
                getJoinType(),
                analyzeCondition().leftKeys.toIntArray(),
                analyzeCondition().rightKeys.toIntArray(),
                nonEquiCondition,
                condition
        );
    }

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors() {
        Map<Integer, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors = new HashMap<>();
        for (Integer i : metadataQuery(this).extractWatermarkedFields(getLeft()).getFieldIndexes()) {
            leftTimeExtractors.put(i, row -> WindowUtils.extractMillis(row.getRow().get(i)));
        }

        return leftTimeExtractors;
    }

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors() {
        Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors = new HashMap<>();
        for (Integer i : metadataQuery(this).extractWatermarkedFields(getRight()).getFieldIndexes()) {
            rightTimeExtractors.put(i, row -> WindowUtils.extractMillis(row.getRow().get(i)));
        }

        return rightTimeExtractors;
    }

    public Map<Integer, Map<Integer, Long>> postponeTimeMap() {
        return postponeTimeMap;
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
