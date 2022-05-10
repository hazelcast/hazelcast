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
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.metadataQuery;
import static java.util.Collections.emptyMap;

public class StreamToStreamJoinPhysicalRel extends JoinPhysicalRel {
    private Tuple3<Integer, Integer, Long> leftBound;
    private Tuple3<Integer, Integer, Long> rightBound;

    protected StreamToStreamJoinPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            Tuple3<Integer, Integer, Long> leftBound,
            Tuple3<Integer, Integer, Long> rightBound
    ) {
        super(cluster, traitSet, left, right, condition, joinType);

        this.leftBound = leftBound;
        this.rightBound = rightBound;
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

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors() {
        WatermarkedFields leftWms = metadataQuery(left).extractWatermarkedFields(left);

        Map<Integer, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors = new HashMap<>();

        for (RexInputRef value : leftWms.getPropertiesByIndex().values()) {
            int i = value.getIndex();
            leftTimeExtractors.put(i, row -> row.getRow().get(i));
        }

        return leftTimeExtractors;
    }

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors() {
        RelNode rightInputConverted = RelRule.convert(right, right.getTraitSet().replace(PHYSICAL));
        WatermarkedFields rightWms = metadataQuery(rightInputConverted).extractWatermarkedFields(rightInputConverted);

        Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors = new HashMap<>();

        for (RexInputRef value : rightWms.getPropertiesByIndex().values()) {
            int i = value.getIndex();
            rightTimeExtractors.put(i, row -> row.getRow().get(i));
        }
        return rightTimeExtractors;
    }

    public Map<Byte, Map<Byte, Long>> postponeTimeMap() {
        return emptyMap();
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
                leftBound,
                rightBound
        );
    }
}