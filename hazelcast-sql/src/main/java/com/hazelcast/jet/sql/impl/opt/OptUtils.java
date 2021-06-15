/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

/**
 * Static utility classes for rules.
 */
public final class OptUtils {

    private OptUtils() {
    }

    /**
     * Convert the given trait set to logical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with logical convention.
     */
    public static RelTraitSet toLogicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, LOGICAL);
    }

    /**
     * Convert the given input into logical input.
     *
     * @param rel Original input.
     * @return Logical input.
     */
    public static RelNode toLogicalInput(RelNode rel) {
        return RelOptRule.convert(rel, toLogicalConvention(rel.getTraitSet()));
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, PHYSICAL);
    }

    /**
     * Convert the given input into physical input.
     *
     * @param rel Original input.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode rel) {
        return RelOptRule.convert(rel, toPhysicalConvention(rel.getTraitSet()));
    }

    /**
     * Add a single trait to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait    Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait) {
        return traitSet.plus(trait).simplify();
    }

    public static LogicalTableScan createLogicalScan(
            RelOptCluster cluster,
            HazelcastTable hazelcastTable
    ) {
        JetTable table = hazelcastTable.getTarget();

        HazelcastRelOptTable relTable = createRelTable(
                table.getQualifiedName(),
                hazelcastTable,
                cluster.getTypeFactory()
        );
        return LogicalTableScan.create(cluster, relTable, ImmutableList.of());
    }

    private static HazelcastRelOptTable createRelTable(
            List<String> names,
            HazelcastTable hazelcastTable,
            RelDataTypeFactory typeFactory
    ) {
        RelDataType rowType = hazelcastTable.getRowType(typeFactory);
        return createRelTable(names, hazelcastTable, rowType);
    }

    public static HazelcastRelOptTable createRelTable(
            List<String> names,
            HazelcastTable hazelcastTable,
            RelDataType rowType
    ) {
        RelOptTableImpl relTable = RelOptTableImpl.create(
                null,
                rowType,
                names,
                hazelcastTable,
                null
        );
        return new HazelcastRelOptTable(relTable);
    }

    public static Collection<RelNode> extractPhysicalRelsFromSubset(RelNode node) {
        if (node instanceof RelSubset) {
            RelSubset subset = (RelSubset) node;

            Set<RelTraitSet> traitSets = new HashSet<>();
            Set<RelNode> result = Collections.newSetFromMap(new IdentityHashMap<>());
            for (RelNode rel : subset.getRelList()) {
                if (!isPhysical(rel)) {
                    continue;
                }

                if (traitSets.add(rel.getTraitSet())) {
                    result.add(RelOptRule.convert(node, rel.getTraitSet()));
                }
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    private static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(JetConventions.PHYSICAL);
    }

    /**
     * If the {@code node} is a {@link RelSubset}, finds the subset matching
     * the {@code operandPredicate}. If multiple or no matches are found,
     * throws an error.
     * <p>
     * If the {@code node} isn't a {@code RelSubset}, check that it matches the
     * predicate and returns it.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> T findMatchingRel(RelNode node, RelOptRuleOperand operandPredicate) {
        if (node instanceof RelSubset) {
            RelNode res = null;
            for (RelNode rel : ((RelSubset) node).getRels()) {
                if (operandPredicate.matches(rel)) {
                    if (res != null) {
                        throw new RuntimeException("multiple matches found");
                    }
                    res = rel;
                }
            }
            if (res != null) {
                return (T) res;
            }
        } else if (operandPredicate.matches(node)) {
            return (T) node;
        }

        throw new RuntimeException("expected rel not found: " + node);
    }

    public static PlanNodeSchema schema(RelOptTable relTable) {
        Table table = relTable.unwrap(HazelcastTable.class).getTarget();

        List<QueryDataType> fieldTypes = new ArrayList<>();
        for (TableField field : table.getFields()) {
            fieldTypes.add(field.getType());
        }
        return new PlanNodeSchema(fieldTypes);
    }

    public static PlanNodeSchema schema(RelDataType rowType) {
        return new PlanNodeSchema(extractFieldTypes(rowType));
    }

    public static RexVisitor<Expression<?>> createRexToExpressionVisitor(
            PlanNodeFieldTypeProvider schema,
            QueryParameterMetadata parameterMetadata
    ) {
        return new RexToExpressionVisitor(schema, parameterMetadata);
    }

    /**
     * Converts a {@link TableField} to {@link RelDataType}.
     */
    public static RelDataType convert(TableField field, RelDataTypeFactory typeFactory) {
        QueryDataType fieldType = field.getType();

        SqlTypeName sqlTypeName = HazelcastTypeUtils.toCalciteType(fieldType);

        if (sqlTypeName == null) {
            throw new IllegalStateException("Unexpected type family: " + fieldType);
        }

        RelDataType relType = typeFactory.createSqlType(sqlTypeName);
        return typeFactory.createTypeWithNullability(relType, true);
    }

    private static List<QueryDataType> extractFieldTypes(RelDataType rowType) {
        return Util.toList(rowType.getFieldList(),
                f -> HazelcastTypeUtils.toHazelcastType(f.getType().getSqlTypeName()));
    }
}
