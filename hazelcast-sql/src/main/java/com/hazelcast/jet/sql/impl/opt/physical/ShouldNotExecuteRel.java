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

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A RelNode that is used to replace another RelNode, if a rule determines that
 * the rel it matched cannot be executed without some transformation. It is used
 * to avoid throwing directly from the rule, where there can perhaps be another
 * rule that can replace the same rel with a rel, that is executable. That's why
 * this rel has huge cost.
 */
public class ShouldNotExecuteRel extends AbstractRelNode implements PhysicalRel {
    private final String exceptionMessage;

    public ShouldNotExecuteRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelDataType type,
            String exceptionMessage) {
        super(cluster, traitSet);
        this.exceptionMessage = exceptionMessage;
        this.rowType = type;
    }

    public String message() {
        return exceptionMessage;
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeHugeCost();
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        throw QueryException.error(SqlErrorCode.GENERIC, message());
    }

    @Override
    @SuppressWarnings("checkstyle:MagicNumber")
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("error", StringUtil.shorten(message(), 30));
    }
}
