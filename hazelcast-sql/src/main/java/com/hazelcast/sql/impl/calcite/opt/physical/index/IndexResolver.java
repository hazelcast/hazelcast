/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastIntegerType;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterType;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class to resolve indexes.
 */
// TODO: Return index scans even if there are no matching predicates because they may provide better collations for parent
//   operators
// TODO: Use monotonicity to employ indexes in more advanced cases, e.g. "WHERE x + 1 > 2"
public final class IndexResolver {
    private IndexResolver() {
        // No-op.
    }

    public static List<RelNode> createIndexScans(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        List<MapTableIndex> indexes
    ) {
        // Early return if there is no filter.
        RexNode filter = scan.getTableUnwrapped().getFilter();

        if (filter == null) {
            return Collections.emptyList();
        }

        // Filter out unsupported indexes.
        List<MapTableIndex> supportedIndexes = new ArrayList<>(indexes.size());
        Set<Integer> allIndexedFieldOrdinals = new HashSet<>();

        for (MapTableIndex index : indexes) {
            if (isIndexSupported(index)) {
                supportedIndexes.add(index);

                allIndexedFieldOrdinals.addAll(index.getFieldOrdinals());
            }
        }

        // Early return if there are no indexes to consider.
        if (supportedIndexes.isEmpty()) {
            return Collections.emptyList();
        }

        // Convert expression into CNF
        IndexConjunctiveFilter cnf = createConjunctiveFilter(filter);

        // Prepare candidates from conjunctive expressions.
        Map<Integer, List<IndexCandidate>> candidates = prepareSingleColumnCandidates(
            cnf.getNodes(),
            OptUtils.getCluster(scan).getParameterMetadata(),
            allIndexedFieldOrdinals
        );

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // Create index rels based on candidates.
        List<RelNode> rels = new ArrayList<>(supportedIndexes.size());

        for (MapTableIndex index : supportedIndexes) {
            RelNode rel = createIndexScan(scan, distribution, index, cnf, candidates);

            if (rel != null) {
                rels.add(rel);
            }
        }

        return rels;
    }

    /**
     * Creates an object for convenient access to part of the predicate in the CNF.
     */
    private static IndexConjunctiveFilter createConjunctiveFilter(RexNode filter) {
        List<RexNode> conjunctions = new ArrayList<>(1);

        RelOptUtil.decomposeConjunction(filter, conjunctions);

        return new IndexConjunctiveFilter(conjunctions);
    }

    /**
     * Creates a map from the scan column ordinal to expressions that could be potentially used by indexes created over
     * this column.
     *
     * @param nodes                   CNF nodes
     * @param allIndexedFieldOrdinals Ordinals of all columns that have some indexes. Helps to filter out candidates that
     *                                definitely cannot be used earlier.
     */
    private static Map<Integer, List<IndexCandidate>> prepareSingleColumnCandidates(
        List<RexNode> nodes,
        QueryParameterMetadata parameterMetadata,
        Set<Integer> allIndexedFieldOrdinals
    ) {
        Map<Integer, List<IndexCandidate>> res = new HashMap<>();

        for (RexNode node : nodes) {
            IndexCandidate candidate = prepareSingleColumnCandidate(node, parameterMetadata);

            if (candidate == null) {
                // Expression cannot be used for indexes
                continue;
            }

            if (!allIndexedFieldOrdinals.contains(candidate.getColumnIndex())) {
                // Expression could be used for indexes, but there are no matching indexes
                continue;
            }

            res.computeIfAbsent(candidate.getColumnIndex(), (k) -> new ArrayList<>()).add(candidate);
        }

        return res;
    }

    /**
     * Prepares an expression candidate for the given RexNode.
     * <p>
     * We consider two types of expressions: comparions predicates and OR condition. We try to interpret OR as IN, otherwise
     * it is ignored.
     *
     * @param exp Calcite expression
     * @return Candidate or {@code null} if the expression cannot be used with indexes.
     */
    private static IndexCandidate prepareSingleColumnCandidate(
        RexNode exp,
        QueryParameterMetadata parameterMetadata
    ) {
        SqlKind kind = exp.getKind();

        switch (kind) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case EQUALS:
                BiTuple<RexNode, RexNode> operands = extractComparisonOperands(exp);

                return prepareSingleColumnCandidateComparison(
                    exp,
                    kind,
                    operands.element1(),
                    operands.element2(),
                    parameterMetadata
                );

            case OR:
                return prepareSingleColumnCandidateOr(
                    exp,
                    ((RexCall) exp).getOperands(),
                    parameterMetadata
                );

            default:
                return null;
        }
    }

    private static IndexCandidate prepareSingleColumnCandidateComparison(
        RexNode exp,
        SqlKind kind,
        RexNode operand1,
        RexNode operand2,
        QueryParameterMetadata parameterMetadata
    ) {
        // Normalize operand positions, so that the column is always goes first
        if (operand1.getKind() != SqlKind.INPUT_REF && operand2.getKind() == SqlKind.INPUT_REF) {
            kind = inverseIndexConditionKind(kind);

            RexNode tmp = operand1;
            operand1 = operand2;
            operand2 = tmp;
        }

        // Exit if the first operand is not a column
        if (operand1.getKind() != SqlKind.INPUT_REF) {
            return null;
        }

        int columnIndex = ((RexInputRef) operand1).getIndex();

        if (!IndexRexVisitor.isValid(operand2)) {
            // The second operand cannot be used for index filter
            return null;
        }

        Expression<?> filterValue = convertToExpression(operand2, parameterMetadata);

        if (filterValue == null) {
            // Operand cannot be converted to expression. Do not throw an exception here, just do not use the faulty condition
            // for index. The proper exception will be thrown on later stages when attempting to convert Calcite rel tree to
            // Hazelcast plan.
            return null;
        }

        return new IndexCandidate(
            exp,
            columnIndex,
            inferConditionType(kind),
            filterValue
        );
    }

    private static Expression<?> convertToExpression(RexNode operand, QueryParameterMetadata parameterMetadata) {
        try {
            RexToExpressionVisitor visitor = new RexToExpressionVisitor(IndexFieldTypeProvider.INSTANCE, parameterMetadata);

            return operand.accept(visitor);
        } catch (Exception e) {
            return null;
        }
    }

    private static IndexCandidate prepareSingleColumnCandidateOr(
        RexNode exp,
        List<RexNode> nodes,
        QueryParameterMetadata parameterMetadata
    ) {
        Integer columnIndex = null;
        List<Expression<?>> values = new ArrayList<>();

        for (RexNode node : nodes) {
            IndexCandidate candidate = prepareSingleColumnCandidate(node, parameterMetadata);

            // Work only with "=" expressions.
            if (candidate == null || candidate.getFilterType() != IndexCandidateType.EQUALS) {
                // TODO: Support index join
                return null;
            }

            // Make sure that all '=' expressions relate to a single column.
            if (columnIndex == null) {
                columnIndex = candidate.getColumnIndex();
            } else if (columnIndex != candidate.getColumnIndex()) {
                return null;
            }

            // Flatten expressions
            Expression<?> value = candidate.getFilterValue();

            if (value instanceof IndexVariExpression) {
                Collections.addAll(values, ((IndexVariExpression) value).getOperands());
            } else {
                assert value instanceof ConstantExpression || value instanceof ParameterExpression;

                values.add(value);
            }
        }

        assert columnIndex != null;

        return new IndexCandidate(
            exp,
            columnIndex,
            IndexCandidateType.IN,
            new IndexVariExpression(values.toArray(new Expression[0]))
        );
    }

    /**
     * Create index scan for the given index if possible.
     *
     * @return Index scan or {@code null}.
     */
    public static RelNode createIndexScan(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        MapTableIndex index,
        IndexConjunctiveFilter cnf,
        Map<Integer, List<IndexCandidate>> candidates
    ) {
        List<IndexFilterDescriptor> filters = new ArrayList<>(index.getFieldOrdinals().size());

        for (int i = 0; i < index.getFieldOrdinals().size(); i++) {
            int fieldOrdinal = index.getFieldOrdinals().get(i);
            QueryDataType fieldConverterType = index.getFieldConverterTypes().get(i);

            List<IndexCandidate> fieldCandidates = candidates.get(fieldOrdinal);

            if (fieldCandidates == null) {
                // No candidates available for the given column, stop.
                break;
            }

            IndexFilterDescriptor filter = createFilterFromCandidates(index.getType(), fieldCandidates, fieldConverterType);

            if (filter == null) {
                // Cannot create a filter for the given candidates, stop.
                break;
            }

            filters.add(filter);

            if (filter.getFilter().getType() != IndexFilterType.EQUALS) {
                // For composite indexes, non-equals condition must always be the last part of the request.
                // If we found non-equals, then we must stop.
                break;
            }
        }

        if (filters.isEmpty()) {
            // Failed to build any filters. The index cannot be used.
            return null;
        }

        // Now as filters are determined, construct the physical entity.
        return createIndexScan(scan, distribution, index, cnf, filters);
    }

    public static RelNode createFullIndexScan(
            MapScanLogicalRel scan,
            DistributionTrait distribution,
            List<MapTableIndex> indexes
    ) {
        assert !indexes.isEmpty();
        MapTableIndex tableIndex = indexes.get(0);

        RexNode scanFilter = scan.getTableUnwrapped().getFilter();

        RelTraitSet traitSet = OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution);

        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.getHazelcastTable(scan);

        RelOptTable newRelTable = OptUtils.createRelTable(
                originalRelTable,
                originalHazelcastTable.withFilter(null),
                scan.getCluster().getTypeFactory()
        );

        return new MapIndexScanPhysicalRel(
                scan.getCluster(),
                traitSet,
                newRelTable,
                tableIndex,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                scanFilter
        );
    }

    private static MapIndexScanPhysicalRel createIndexScan(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        MapTableIndex index,
        IndexConjunctiveFilter cnf,
        List<IndexFilterDescriptor> filterDescriptors
    ) {
        // Collect filters and relevant expressions
        List<IndexFilter> filters = new ArrayList<>(filterDescriptors.size());
        List<QueryDataType> converterTypes = new ArrayList<>(filterDescriptors.size());
        Set<RexNode> exps = new HashSet<>();

        for (IndexFilterDescriptor filterDescriptor : filterDescriptors) {
            filters.add(filterDescriptor.getFilter());
            converterTypes.add(filterDescriptor.getConverterType());
            exps.addAll(filterDescriptor.getExpressions());
        }

        // Construct Calcite expressions.
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

        RexNode exp = RexUtil.composeConjunction(rexBuilder, exps);
        RexNode remainderExp = RexUtil.composeConjunction(rexBuilder, cnf.exclude(exps));

        // Prepare traits
        RelTraitSet traitSet = OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution);

        // TODO: We must add collation here (see commented line). Somehow it breaks the planner.
//        RelCollation collation = createIndexCollation(scan, index);
//        RelTraitSet traitSet = RuleUtils.toPhysicalConvention(scan.getTraitSet(), distribution).plus(collation);

        // Create the index scan
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.getHazelcastTable(scan);

        RelOptTable newRelTable = OptUtils.createRelTable(
            originalRelTable,
            originalHazelcastTable.withFilter(null),
            scan.getCluster().getTypeFactory()
        );

        return new MapIndexScanPhysicalRel(
            scan.getCluster(),
            traitSet,
            newRelTable,
            index,
            filters,
            converterTypes,
            exp,
            remainderExp
        );
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static IndexFilterDescriptor createFilterFromCandidates(
        IndexType type,
        List<IndexCandidate> candidates,
        QueryDataType converterType
    ) {
        // First look for equality conditions, assuming that it is the most restrictive
        for (IndexCandidate candidate : candidates) {
            if (candidate.getFilterType() == IndexCandidateType.EQUALS) {
                IndexFilter filter = IndexFilter.forEquals(candidate.getFilterValue());

                return new IndexFilterDescriptor(filter, candidate.getExpression(), converterType);
            }
        }

        // Next look for IN, as it is worse than equality on a single value, but better than range
        for (IndexCandidate candidate : candidates) {
            if (candidate.getFilterType() == IndexCandidateType.IN) {
                // TODO: Make sure that the order is preserved here! It seems that the values should be sorted
                //   when accessing the index store.
                IndexFilter filter = IndexFilter.forIn(candidate.getFilterValue());

                return new IndexFilterDescriptor(filter, candidate.getExpression(), converterType);
            }
        }

        // Last, look for ranges
        if (type == IndexType.SORTED) {
            Expression<?> from = null;
            boolean fromInclusive = false;
            Expression<?> to = null;
            boolean toInclusive = false;
            List<RexNode> expressions = new ArrayList<>(2);

            for (IndexCandidate candidate : candidates) {
                switch (candidate.getFilterType()) {
                    case GREATER_THAN:
                        if (from == null) {
                            from = candidate.getFilterValue();
                            fromInclusive = false;
                            expressions.add(candidate.getExpression());
                        }

                        break;

                    case GREATER_THAN_OR_EQUALS:
                        if (from == null) {
                            from = candidate.getFilterValue();
                            fromInclusive = true;
                            expressions.add(candidate.getExpression());
                        }

                        break;

                    case LESS_THAN:
                        if (to == null) {
                            to = candidate.getFilterValue();
                            toInclusive = false;
                            expressions.add(candidate.getExpression());
                        }

                        break;

                    default:
                        assert candidate.getFilterType() == IndexCandidateType.LESS_THAN_OR_EQUALS;

                        if (to == null) {
                            to = candidate.getFilterValue();
                            toInclusive = true;
                            expressions.add(candidate.getExpression());
                        }

                        break;
                }
            }

            if (from != null || to != null) {
                IndexFilter filter = IndexFilter.forRange(from, fromInclusive, to, toInclusive);

                return new IndexFilterDescriptor(filter, expressions, converterType);
            }
        }

        // Cannot create an index request for the given candidates
        return null;
    }

    private static IndexCandidateType inferConditionType(SqlKind kind) {
        switch (kind) {
            case GREATER_THAN:
                return IndexCandidateType.GREATER_THAN;

            case GREATER_THAN_OR_EQUAL:
                return IndexCandidateType.GREATER_THAN_OR_EQUALS;

            case LESS_THAN:
                return IndexCandidateType.LESS_THAN;

            case LESS_THAN_OR_EQUAL:
                return IndexCandidateType.LESS_THAN_OR_EQUALS;

            default:
                assert kind == SqlKind.EQUALS;

                return IndexCandidateType.EQUALS;
        }
    }

    private static SqlKind inverseIndexConditionKind(SqlKind kind) {
        switch (kind) {
            case GREATER_THAN:
                return SqlKind.LESS_THAN;

            case GREATER_THAN_OR_EQUAL:
                return SqlKind.LESS_THAN_OR_EQUAL;

            case LESS_THAN:
                return SqlKind.GREATER_THAN;

            case LESS_THAN_OR_EQUAL:
                return SqlKind.GREATER_THAN_OR_EQUAL;

            default:
                assert kind == SqlKind.EQUALS;

                return kind;
        }
    }

    /**
     * Create collation trait for the given scan and index.
     *
     * @param scan  Scan.
     * @param index Index.
     * @return Collation trait.
     */
    // TODO: Proper collation integration for indexes! Not used now, see TODO near trait construction
    private static RelCollation createIndexCollation(MapScanLogicalRel scan, MapTableIndex index) {
        if (index.getType() == IndexType.HASH) {
            // Hash index doesn't enforce any collation.
            return RelCollations.EMPTY;
        }

        assert index.getType() == IndexType.SORTED;

        // Map scan field names to relevant outputs.
        Map<Integer, Integer> fieldToProjectIndex = new HashMap<>();

        for (int i = 0; i < scan.getMap().getFieldCount(); i++) {
            int projectIndex = scan.getTableUnwrapped().getProjects().indexOf(i);

            if (projectIndex == -1) {
                // Scan field is not projected out.
                continue;
            }

            fieldToProjectIndex.put(i, projectIndex);
        }

        // Now add prefix of index attributes.
        List<RelFieldCollation> fieldCollations = new ArrayList<>(index.getFieldOrdinals().size());

        for (Integer indexFieldOrdinal : index.getFieldOrdinals()) {
            Integer projectIndex = fieldToProjectIndex.get(indexFieldOrdinal);

            if (projectIndex == null) {
                // Collation field is not present in the output. Further sorting is impossible.
                break;
            }

            // We support only ascending direction at the moment.
            RelFieldCollation fieldCollation = new RelFieldCollation(projectIndex, RelFieldCollation.Direction.ASCENDING);

            fieldCollations.add(fieldCollation);
        }

        return fieldCollations.isEmpty() ? RelCollations.EMPTY : RelCollations.of(fieldCollations);
    }

    private static boolean isIndexSupported(MapTableIndex index) {
        return index.getType() == IndexType.SORTED || index.getType() == IndexType.HASH;
    }

    /**
     * Extracts comparison operands, removing CAST when possible.
     *
     * @param node original comparison node
     * @return a pair of operands
     */
    private static BiTuple<RexNode, RexNode> extractComparisonOperands(RexNode node) {
        assert node instanceof RexCall;

        RexCall node0 = (RexCall) node;

        assert node0.getOperands().size() == 2;

        RexNode operand1 = node0.getOperands().get(0);
        RexNode operand2 = node0.getOperands().get(1);

        RexNode normalizedOperand1 = removeCastIfPossible(operand1);
        RexNode normalizedOperand2 = removeCastIfPossible(operand2);

        return BiTuple.of(normalizedOperand1, normalizedOperand2);
    }

    private static RexNode removeCastIfPossible(RexNode node) {
        if (node.getKind() == SqlKind.CAST) {
            RexCall node0 = (RexCall) node;

            RexNode from = node0.getOperands().get(0);

            RelDataType fromType = from.getType();
            RelDataType toType = node0.getType();

            // No conversion => remove CAST
            if (fromType.equals(toType)) {
                return from;
            }

            // Converting between integer types => remove CAST
            if (fromType instanceof HazelcastIntegerType && toType instanceof HazelcastIntegerType) {
                return from;
            }

            // TODO: Strings?
        }

        return node;
    }
}
