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
import com.hazelcast.jet.sql.impl.opt.Conventions;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Collections.nCopies;

/**
 * Marker interface for physical relations.
 */
public interface PhysicalRel extends PhysicalNode {

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

    /**
     * Propagates plan traits during first (top-down) phase.
     * We are using default implementation, which fits well.
     */
    @Override
    @Nullable
    default RelNode passThrough(RelTraitSet required) {
        return PhysicalNode.super.passThrough(required);
    }

    /**
     * Propagates plan traits during first (top-down) phase for every child separately.
     * By default, we're just forward existing traits below.
     *
     * @param required input (parent) traits
     * @return Pair(current rel traitset, the list of required traitsets for child nodes)
     */
    @Override
    @Nullable
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        RelNode rel = this;

        // Note: it's a wild cheating, but ...
        required = required.replace(Conventions.PHYSICAL);

        // We don't work with non-physical search space, and we stop working on leaf rels.
        if (required.getConvention() != Conventions.PHYSICAL || rel.getInputs().isEmpty()) {
            return null;
        }

        RelTraitSet relTraitSet = passThroughCollationTraits(rel, required);

        List<RelTraitSet> relTraitSetsToPropagate = nCopies(rel.getInputs().size(), relTraitSet);
        return Pair.of(relTraitSet, relTraitSetsToPropagate);
    }

    /**
     * Propagates collation trait during first (top-down) phase.
     */
    default RelTraitSet passThroughCollationTraits(RelNode rel, RelTraitSet required) {
        RelCollation collationTrait = OptUtils.getCollation(rel);
        return required.replace(collationTrait);
    }

    /**
     * Derives plan traits during second (bottom-up, optimization) phase.
     * We are using {@link DeriveMode.LEFT_FIRST} derivation mode, so,
     * the default implementation fits well for most relations.
     */
    @SuppressWarnings("JavadocReference")
    @Override
    @Nullable
    default RelNode derive(RelTraitSet childTraits, int childId) {
        return PhysicalNode.super.derive(childTraits, childId);
    }

    @Override
    @Nullable
    default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return PhysicalNode.super.deriveTraits(childTraits, childId);
    }

    /**
     * Uses the left most child's traits to decide what
     * traits to require from the other children.
     * This generally applies to most operators.
     */
    @Override
    default DeriveMode getDeriveMode() {
        return DeriveMode.LEFT_FIRST;
    }
}

