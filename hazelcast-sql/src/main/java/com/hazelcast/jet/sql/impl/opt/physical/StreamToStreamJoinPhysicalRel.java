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
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class StreamToStreamJoinPhysicalRel extends JoinPhysicalRel {
    private final WatermarkedFields leftWatermarkedFields;
    private final WatermarkedFields rightWatermarkedFields;
    // Same postponeTimeMap as required by TDD, but with rel field defined
    // instead of Jet's watermark key, which will be computed on CreateDagVisitor level.
    private final Map<OptUtils.RelField, Map<OptUtils.RelField, Long>> postponeTimeMap;

    @SuppressWarnings("checkstyle:ParameterNumber")
    protected StreamToStreamJoinPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            WatermarkedFields leftWatermarkedFields,
            WatermarkedFields rightWatermarkedFields,
            Map<OptUtils.RelField, Map<OptUtils.RelField, Long>> postponeTimeMap
    ) {
        super(cluster, traitSet, left, right, condition, joinType);

        this.leftWatermarkedFields = leftWatermarkedFields;
        this.rightWatermarkedFields = rightWatermarkedFields;

        this.postponeTimeMap = postponeTimeMap;
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
        Map<Integer, ToLongFunctionEx<JetSqlRow>> leftTimeExtractors = new HashMap<>();
        for (RexInputRef value : leftWatermarkedFields.getPropertiesByIndex().values()) {
            int i = value.getIndex();
            QueryDataType dataType = HazelcastTypeUtils.toHazelcastType(value.getType());
            assert dataType.getTypeFamily().isTemporal() : "Field " + i + " is not temporal! Can't extract timestamp!";

            // TODO: enhance conversion ?
            leftTimeExtractors.put(i, row -> Timestamp.from(Instant.from(row.getRow().get(i))).getTime());
        }

        return leftTimeExtractors;
    }

    public Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors() {
        Map<Integer, ToLongFunctionEx<JetSqlRow>> rightTimeExtractors = new HashMap<>();
        for (RexInputRef value : rightWatermarkedFields.getPropertiesByIndex().values()) {
            int i = value.getIndex();
            QueryDataType dataType = HazelcastTypeUtils.toHazelcastType(value.getType());
            assert dataType.getTypeFamily().isTemporal() : "Field " + i + " is not temporal! Can't extract timestamp!";

            // TODO: enhance conversion ?
            rightTimeExtractors.put(i, row -> Timestamp.from(Instant.from(row.getRow().get(i))).getTime());
        }
        return rightTimeExtractors;
    }

    /**
     * SELECT * FROM ...
     * JOIN ON b.1 BETWEEN a.1 AND a.1 + 1
     * JOIN ON c.1 BETWEEN b.1 AND b.1 + 1
     */
    public Map<OptUtils.RelField, Map<OptUtils.RelField, Long>> postponeTimeMap() {
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
                leftWatermarkedFields,
                rightWatermarkedFields,
                postponeTimeMap
        );
    }
}