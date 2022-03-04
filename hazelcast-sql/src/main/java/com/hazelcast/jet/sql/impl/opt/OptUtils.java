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

package com.hazelcast.jet.sql.impl.opt;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.opt.distribution.DistributionTrait;
import com.hazelcast.jet.sql.impl.opt.metadata.Boundedness;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastJsonType;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.HazelcastRelSubsetUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static com.hazelcast.jet.impl.util.Util.arrayIndexOf;
import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

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

    public static LogicalTableScan createLogicalScan(
            TableScan originalScan,
            HazelcastTable newHazelcastTable
    ) {
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) originalScan.getTable();

        HazelcastRelOptTable newTable = createRelTable(
                originalRelTable,
                newHazelcastTable,
                originalScan.getCluster().getTypeFactory()
        );

        return LogicalTableScan.create(
                originalScan.getCluster(),
                newTable,
                originalScan.getHints()
        );
    }

    public static HazelcastRelOptTable createRelTable(
            List<String> names,
            HazelcastTable hazelcastTable,
            RelDataTypeFactory typeFactory
    ) {
        RelDataType rowType = hazelcastTable.getRowType(typeFactory);

        RelOptTableImpl relTable = RelOptTableImpl.create(
                null,
                rowType,
                names,
                hazelcastTable,
                null
        );
        return new HazelcastRelOptTable(relTable);
    }

    public static HazelcastRelOptTable createRelTable(
            HazelcastRelOptTable originalRelTable,
            HazelcastTable newHazelcastTable,
            RelDataTypeFactory typeFactory
    ) {
        RelOptTableImpl newTable = RelOptTableImpl.create(
                originalRelTable.getRelOptSchema(),
                newHazelcastTable.getRowType(typeFactory),
                originalRelTable.getDelegate().getQualifiedName(),
                newHazelcastTable,
                null
        );

        return new HazelcastRelOptTable(newTable);
    }

    /**
     * Finds a set for the given RelNode, and return subsets that have the
     * physical trait. Every returned input is guaranteed to have a unique trait
     * set.
     *
     * @return Physical rels.
     */
    public static Collection<RelNode> extractPhysicalRelsFromSubset(RelNode input) {
        return extractRelsFromSubset(input, OptUtils::isPhysical);
    }

    private static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(Conventions.PHYSICAL);
    }

    /**
     * Finds a set for the given RelNode, and return subsets that have the
     * logical trait. Every returned input is guaranteed to have a unique trait
     * set.
     *
     * @return Logical rels.
     */
    public static Collection<RelNode> extractLogicalRelsFromSubset(RelNode input) {
        return extractRelsFromSubset(input, OptUtils::isLogical);
    }

    private static boolean isLogical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(Conventions.LOGICAL);
    }

    private static Collection<RelNode> extractRelsFromSubset(RelNode input, Predicate<RelNode> predicate) {
        Set<RelTraitSet> traitSets = new HashSet<>();

        Set<RelNode> res = Collections.newSetFromMap(new IdentityHashMap<>());

        for (RelNode rel : HazelcastRelSubsetUtil.getSubsets(input)) {
            if (!predicate.test(rel)) {
                continue;
            }

            if (traitSets.add(rel.getTraitSet())) {
                res.add(rel);
            }
        }

        return res;
    }

    public static boolean isBounded(RelNode rel) {
        return metadataQuery(rel).extractBoundedness(rel) == Boundedness.BOUNDED;
    }

    public static boolean isUnbounded(RelNode rel) {
        return metadataQuery(rel).extractBoundedness(rel) == Boundedness.UNBOUNDED;
    }

    public static HazelcastRelMetadataQuery metadataQuery(RelNode rel) {
        return HazelcastRelMetadataQuery.reuseOrCreate(rel.getCluster().getMetadataQuery());
    }

    public static HazelcastRelOptCluster getCluster(RelNode rel) {
        assert rel.getCluster() instanceof HazelcastRelOptCluster;

        return (HazelcastRelOptCluster) rel.getCluster();
    }

    public static DistributionTrait getDistribution(RelNode rel) {
        return rel.getTraitSet().getTrait(getCluster(rel).getDistributionTraitDef());
    }

    /**
     * If the {@code node} is a {@link RelSubset}, finds the subset matching
     * the {@code operandPredicate}.
     * If multiple matches are found, it throws. If no match is found, it returns null.
     * <p>
     * If the {@code node} isn't a {@code RelSubset} and it matches the
     * predicate, the {@code node} is returned. If it doesn't match,
     * {@code null} is returned.
     */
    @SuppressWarnings("unchecked")
    @Nullable
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
            return (T) res;
        } else if (operandPredicate.matches(node)) {
            return (T) node;
        }
        return null;
    }

    public static PlanNodeSchema schema(RelDataType rowType) {
        return new PlanNodeSchema(extractFieldTypes(rowType));
    }

    public static PlanNodeSchema schema(RelOptTable relTable) {
        Table table = relTable.unwrap(HazelcastTable.class).getTarget();
        return schema(table);
    }

    public static PlanNodeSchema schema(Table table) {
        List<QueryDataType> fieldTypes = new ArrayList<>();
        for (TableField field : table.getFields()) {
            fieldTypes.add(field.getType());
        }
        return new PlanNodeSchema(fieldTypes);
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
            throw new IllegalStateException("Unsupported type family: " + fieldType
                    + ", getSqlTypeName should never return null.");
        }

        if (sqlTypeName == SqlTypeName.OTHER) {
            return convertCustomType(fieldType);
        } else {
            RelDataType relType = typeFactory.createSqlType(sqlTypeName);
            return typeFactory.createTypeWithNullability(relType, true);
        }
    }

    private static RelDataType convertCustomType(QueryDataType fieldType) {
        switch (fieldType.getTypeFamily()) {
            case JSON:
                return HazelcastJsonType.create(true);
            default:
                throw new IllegalStateException("Unexpected type family: " + fieldType);
        }
    }

    private static List<QueryDataType> extractFieldTypes(RelDataType rowType) {
        return Util.toList(rowType.getFieldList(),
                f -> HazelcastTypeUtils.toHazelcastType(f.getType()));
    }

    public static boolean requiresJob(RelNode rel) {
        return ((HazelcastRelOptCluster) rel.getCluster()).requiresJob();
    }

    public static boolean hasTableType(RelNode rel, Class<? extends Table> tableClass) {
        if (rel.getTable() == null) {
            return false;
        }

        HazelcastTable table = rel.getTable().unwrap(HazelcastTable.class);
        return table != null && tableClass.isAssignableFrom(table.getTarget().getClass());
    }

    public static HazelcastTable extractHazelcastTable(RelNode rel) {
        HazelcastTable table = rel.getTable().unwrap(HazelcastTable.class);
        assert table != null;
        return table;
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    public static RexNode extractKeyConstantExpression(RelOptTable relTable, RexBuilder rexBuilder) {
        HazelcastTable table = relTable.unwrap(HazelcastTable.class);

        RexNode filter = table.getFilter();
        if (filter == null) {
            return null;
        }

        int keyIndex = findKeyIndex(table.getTarget());
        switch (filter.getKind()) {
            // WHERE __key = true, calcite simplifies to just `WHERE __key`
            case INPUT_REF: {
                return ((RexInputRef) filter).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(true)
                        : null;
            }
            // WHERE __key = false, calcite simplifies to `WHERE NOT __key`
            case NOT: {
                RexNode operand = ((RexCall) filter).getOperands().get(0);
                return operand.getKind() == SqlKind.INPUT_REF && ((RexInputRef) operand).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(false)
                        : null;
            }
            // __key = ...
            case EQUALS: {
                Tuple2<Integer, RexNode> constantExpressionByIndex = extractConstantExpression((RexCall) filter);
                //noinspection ConstantConditions
                return constantExpressionByIndex != null && constantExpressionByIndex.getKey() == keyIndex
                        ? constantExpressionByIndex.getValue()
                        : null;
            }
            default:
                return null;
        }
    }

    private static int findKeyIndex(Table table) {
        List<String> primaryKey = SqlConnectorUtil.getJetSqlConnector(table).getPrimaryKey(table);
        // just single field keys supported at the moment
        assert primaryKey.size() == 1;

        int keyIndex = table.getFieldIndex(primaryKey.get(0));
        assert keyIndex > -1;

        return keyIndex;
    }

    private static Tuple2<Integer, RexNode> extractConstantExpression(RexCall condition) {
        Tuple2<Integer, RexNode> constantExpression = extractConstantExpression(condition, 0);
        return constantExpression != null ? constantExpression : extractConstantExpression(condition, 1);
    }

    private static Tuple2<Integer, RexNode> extractConstantExpression(RexCall condition, int i) {
        RexNode firstOperand = condition.getOperands().get(i);
        if (firstOperand.getKind() == SqlKind.INPUT_REF) {
            int index = ((RexInputRef) firstOperand).getIndex();
            RexNode secondOperand = condition.getOperands().get(1 - i);
            if (RexUtil.isConstant(secondOperand)) {
                return Tuple2.tuple2(index, secondOperand);
            }
        }
        return null;
    }

    /**
     * Return true if the `expression` contains any input reference to a field
     * with index in `indexes`.
     */
    public static boolean hasInputRef(RexNode expression, int... indexes) {
        boolean[] res = {false};
        expression.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                if (arrayIndexOf(inputRef.getIndex(), indexes) >= 0) {
                    res[0] = true;
                }
                return null;
            }
        });
        return res[0];
    }

    /**
     * Inlines `inlinedExpressions` into `expr` and returns the modified expression.
     * <p>
     * Example:
     * {@code
     * inlinedExpressions: [UPPER($1), LOWER($0)]
     * expr: $1 || $0
     * result: LOWER($0) || UPPER($1)
     * }
     */
    @SuppressWarnings("checkstyle:AnonInnerLength")
    public static RexNode inlineExpression(List<RexNode> inlinedExpressions, RexNode expr) {
        return expr.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                return inlinedExpressions.get(inputRef.getIndex());
            }
        });
    }

    /**
     * Same as {@link #inlineExpression(List, RexNode)}, but applied to all
     * expressions in {@code exprs}.
     */
    public static List<RexNode> inlineExpressions(List<RexNode> inlinedExpressions, List<RexNode> exprs) {
        List<RexNode> res = new ArrayList<>(exprs.size());
        for (RexNode expr : exprs) {
            res.add(inlineExpression(inlinedExpressions, expr));
        }
        return res;
    }
}
