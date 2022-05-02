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
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;

public class StreamToStreamJoinPhysicalRel extends JoinPhysicalRel {
    protected StreamToStreamJoinPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, joinType);
    }

    public JetJoinInfo joinInfo(QueryParameterMetadata parameterMetadata) {
        RexNode predicate = analyzeCondition().getRemaining(getCluster().getRexBuilder());
        Expression<Boolean> nonEquiCondition = filter(schema(parameterMetadata), predicate, parameterMetadata);

        RexNode equiJoinCondition = analyzeCondition().getEquiCondition(left, right, getCluster().getRexBuilder());
        Expression<Boolean> equiCondition = filter(schema(parameterMetadata), equiJoinCondition, parameterMetadata);

        return new JetJoinInfo(
                getJoinType(),
                analyzeCondition().leftKeys.toIntArray(),
                analyzeCondition().rightKeys.toIntArray(),
                nonEquiCondition,
                equiCondition
        );
    }

    public Map<Byte, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors() {
        // TODO: implement it
        return Collections.singletonMap((byte) 0, row -> row.getRow().get(0));
    }

    public Map<Byte, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors() {
        // TODO: implement it
        return Collections.singletonMap((byte) 1, row -> row.getRow().get(0));
    }

    public Map<Byte, Map<Byte, Long>> postponeTimeMap() {
        // TODO: implement it
        Map<Byte, Map<Byte, Long>> postponeTimeMap = new HashMap<>();
        postponeTimeMap.put((byte) 0, singletonMap((byte) 0, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 1, 0L));
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
                joinType
        );
    }
}
