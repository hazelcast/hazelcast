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

import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.util.List;

/**
 * Marker interface for physical relations.
 */
public interface PhysicalRel extends RelNode {

    PlanNodeSchema schema(QueryParameterMetadata parameterMetadata);

    @SuppressWarnings("unchecked")
    default Expression<Boolean> filter(
            PlanNodeFieldTypeProvider schema,
            RexNode node,
            QueryParameterMetadata parameterMetadata
    ) {
        if (node == null) {
            return null;
        }
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return (Expression<Boolean>) node.accept(visitor);
    }

    default List<Expression<?>> project(
            PlanNodeFieldTypeProvider schema,
            List<? extends RexNode> nodes,
            QueryParameterMetadata parameterMetadata
    ) {
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema, parameterMetadata);
        return Util.toList(nodes, node -> node.accept(visitor));
    }

    @Override
    default RelDataType getRowType() {
        return getTable().unwrap(HazelcastTable.class).getRowType(getCluster().getTypeFactory());
    }

    /**
     * Accept a visitor to this physical rel.
     *
     * @param visitor Visitor.
     * @return the DAG vertex created for this rel
     */
    Vertex accept(CreateDagVisitor visitor);
}
