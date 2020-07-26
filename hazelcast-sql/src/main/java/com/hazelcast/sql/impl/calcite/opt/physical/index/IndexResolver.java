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
import com.hazelcast.query.impl.ComparableIdentifiedDataSerializable;
import com.hazelcast.query.impl.TypeConverters;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexInFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
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
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static java.util.Collections.singletonList;
import static org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC;

/**
 * Helper class to resolve indexes.
 */
@SuppressWarnings("rawtypes")
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
        List<RexNode> conjunctions = createConjunctiveFilter(filter);

        // Prepare candidates from conjunctive expressions.
        Map<Integer, List<IndexComponentCandidate>> candidates = prepareSingleColumnCandidates(
            conjunctions,
            OptUtils.getCluster(scan).getParameterMetadata(),
            allIndexedFieldOrdinals
        );

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // Create index relational operators based on candidates.
        List<RelNode> rels = new ArrayList<>(supportedIndexes.size());

        for (MapTableIndex index : supportedIndexes) {
            RelNode rel = createIndexScan(scan, distribution, index, conjunctions, candidates);

            if (rel != null) {
                rels.add(rel);
            }
        }

        return rels;
    }

    /**
     * Creates an object for convenient access to part of the predicate in the CNF.
     */
    private static List<RexNode> createConjunctiveFilter(RexNode filter) {
        List<RexNode> conjunctions = new ArrayList<>(1);

        RelOptUtil.decomposeConjunction(filter, conjunctions);

        return conjunctions;
    }

    /**
     * Creates a map from the scan column ordinal to expressions that could be potentially used by indexes created over
     * this column.
     *
     * @param nodes                   CNF nodes
     * @param allIndexedFieldOrdinals Ordinals of all columns that have some indexes. Helps to filter out candidates that
     *                                definitely cannot be used earlier.
     */
    private static Map<Integer, List<IndexComponentCandidate>> prepareSingleColumnCandidates(
        List<RexNode> nodes,
        QueryParameterMetadata parameterMetadata,
        Set<Integer> allIndexedFieldOrdinals
    ) {
        Map<Integer, List<IndexComponentCandidate>> res = new HashMap<>();

        for (RexNode node : nodes) {
            IndexComponentCandidate candidate = prepareSingleColumnCandidate(node, parameterMetadata);

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
     * We consider two types of expressions: comparison predicates and OR condition. We try to interpret OR as IN, otherwise
     * it is ignored.
     *
     * @param exp calcite expression
     * @return candidate or {@code null} if the expression cannot be used with indexes
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static IndexComponentCandidate prepareSingleColumnCandidate(
        RexNode exp,
        QueryParameterMetadata parameterMetadata
    ) {
        SqlKind kind = exp.getKind();

        switch (kind) {
            case INPUT_REF:
                // Special case for boolean columns: SELECT * FROM t WHERE f_boolean
                return prepareSingleColumnCandidateBooleanIsTrueFalse(exp, exp, SqlKind.IS_TRUE);

            case IS_TRUE:
            case IS_FALSE:
            case IS_NOT_TRUE:
            case IS_NOT_FALSE:
                return prepareSingleColumnCandidateBooleanIsTrueFalse(
                    exp,
                    removeCastIfPossible(((RexCall) exp).getOperands().get(0), false),
                    kind
                );

            case NOT:
                return prepareSingleColumnCandidateBooleanIsTrueFalse(
                    exp,
                    removeCastIfPossible(((RexCall) exp).getOperands().get(0), false),
                    SqlKind.IS_FALSE
                );

            case IS_NULL:
                return prepareSingleColumnCandidateIsNull(
                    exp,
                    removeCastIfPossible(((RexCall) exp).getOperands().get(0), true)
                );

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

    /**
     * Prepare a candidate for {@code IS (NOT) TRUE/FALSE} expression.
     * <p>
     * The fundamental observation is that boolean column may have only three values - TRUE/FALSE/NULL. Therefore, every
     * such expression could be converted to equivalent equals or IN predicate.
     *
     * @param exp expression
     * @param operand operand with CAST unwrapped
     * @param kind expression type
     * @return candidate or {@code null}
     */
    private static IndexComponentCandidate prepareSingleColumnCandidateBooleanIsTrueFalse(
        RexNode exp,
        RexNode operand,
        SqlKind kind
    ) {
        if (operand.getKind() != SqlKind.INPUT_REF) {
            return null;
        }

        if (operand.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
            // Only boolean columns could be used with this optimization
            return null;
        }

        int columnIndex = ((RexInputRef) operand).getIndex();

        IndexFilter filter;

        switch (kind) {
            case IS_TRUE:
                filter = new IndexEqualsFilter(new IndexFilterValue(
                    singletonList(ConstantExpression.create(true, QueryDataType.BOOLEAN)), singletonList(false)
                ));

                break;

            case IS_FALSE:
                filter = new IndexEqualsFilter(new IndexFilterValue(
                    singletonList(ConstantExpression.create(false, QueryDataType.BOOLEAN)), singletonList(false)
                ));

                break;

            case IS_NOT_TRUE:
                filter = new IndexInFilter(
                    new IndexEqualsFilter(new IndexFilterValue(
                        singletonList(ConstantExpression.create(false, QueryDataType.BOOLEAN)), singletonList(false)
                    )),
                    new IndexEqualsFilter(new IndexFilterValue(
                        singletonList(ConstantExpression.create(null, QueryDataType.BOOLEAN)), singletonList(true)
                    ))
                );

                break;

            default:
                assert kind == SqlKind.IS_NOT_FALSE;

                filter = new IndexInFilter(
                    new IndexEqualsFilter(new IndexFilterValue(
                        singletonList(ConstantExpression.create(true, QueryDataType.BOOLEAN)), singletonList(false)
                    )),
                    new IndexEqualsFilter(new IndexFilterValue(
                        singletonList(ConstantExpression.create(null, QueryDataType.BOOLEAN)), singletonList(true)
                    ))
                );
        }

        return new IndexComponentCandidate(exp, columnIndex, filter);
    }

    private static IndexComponentCandidate prepareSingleColumnCandidateIsNull(RexNode exp, RexNode operand) {
        if (operand.getKind() != SqlKind.INPUT_REF) {
            return null;
        }

        int columnIndex = ((RexInputRef) operand).getIndex();

        QueryDataType type = SqlToQueryType.map(operand.getType().getSqlTypeName());

        IndexFilterValue filterValue = new IndexFilterValue(
            singletonList(ConstantExpression.create(null, type)),
            singletonList(true)
        );

        IndexFilter filter = new IndexEqualsFilter(filterValue);

        return new IndexComponentCandidate(
            exp,
            columnIndex,
            filter
        );
    }

    private static IndexComponentCandidate prepareSingleColumnCandidateComparison(
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

        IndexFilterValue filterValue0 = new IndexFilterValue(
            singletonList(filterValue),
            singletonList(false)
        );

        IndexFilter filter;

        switch (kind) {
            case EQUALS:
                filter = new IndexEqualsFilter(filterValue0);

                break;

            case GREATER_THAN:
                filter = new IndexRangeFilter(filterValue0, false, null, false);

                break;

            case GREATER_THAN_OR_EQUAL:
                filter = new IndexRangeFilter(filterValue0, true, null, false);

                break;

            case LESS_THAN:
                filter = new IndexRangeFilter(null, false, filterValue0, false);

                break;

            default:
                assert kind == SqlKind.LESS_THAN_OR_EQUAL;

                filter = new IndexRangeFilter(null, false, filterValue0, true);
        }

        return new IndexComponentCandidate(
            exp,
            columnIndex,
            filter
        );
    }

    private static Expression<?> convertToExpression(RexNode operand, QueryParameterMetadata parameterMetadata) {
        try {
            RexToExpressionVisitor visitor = new RexToExpressionVisitor(FieldTypeProvider.INSTANCE, parameterMetadata);

            return operand.accept(visitor);
        } catch (Exception e) {
            return null;
        }
    }

    private static IndexComponentCandidate prepareSingleColumnCandidateOr(
        RexNode exp,
        List<RexNode> nodes,
        QueryParameterMetadata parameterMetadata
    ) {
        Integer columnIndex = null;

        List<IndexFilter> filters = new ArrayList<>();

        for (RexNode node : nodes) {
            IndexComponentCandidate candidate = prepareSingleColumnCandidate(node, parameterMetadata);

            if (candidate == null) {
                // Cannot resolve further, stop
                return null;
            }

            // Work only with "=" expressions.
            IndexFilter candidateFilter = candidate.getFilter();

            if (!(candidateFilter instanceof IndexEqualsFilter || candidateFilter instanceof IndexInFilter)) {
                // Support only equality for ORs
                return null;
            }

            // Make sure that all '=' expressions relate to a single column
            if (columnIndex == null) {
                columnIndex = candidate.getColumnIndex();
            } else if (columnIndex != candidate.getColumnIndex()) {
                return null;
            }

            // Flatten
            if (candidateFilter instanceof IndexEqualsFilter) {
                filters.add(candidateFilter);
            } else {
                filters.addAll(((IndexInFilter) candidateFilter).getFilters());
            }
        }

        assert columnIndex != null;

        IndexInFilter inFilter = new IndexInFilter(filters);

        return new IndexComponentCandidate(
            exp,
            columnIndex,
            inFilter
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
        List<RexNode> conjunctions,
        Map<Integer, List<IndexComponentCandidate>> candidates
    ) {
        List<IndexComponentFilter> filters = new ArrayList<>(index.getFieldOrdinals().size());

        for (int i = 0; i < index.getFieldOrdinals().size(); i++) {
            int fieldOrdinal = index.getFieldOrdinals().get(i);
            QueryDataType fieldConverterType = index.getFieldConverterTypes().get(i);

            List<IndexComponentCandidate> fieldCandidates = candidates.get(fieldOrdinal);

            if (fieldCandidates == null) {
                // No candidates available for the given column, stop.
                break;
            }

            IndexComponentFilter filter = selectComponentFilter(
                index.getType(),
                fieldCandidates,
                fieldConverterType
            );

            if (filter == null) {
                // Cannot create a filter for the given candidates, stop.
                break;
            }

            filters.add(filter);

            if (!(filter.getFilter() instanceof IndexEqualsFilter)) {
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
        return createIndexScan(scan, distribution, index, conjunctions, filters);
    }

    private static MapIndexScanPhysicalRel createIndexScan(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        MapTableIndex index,
        List<RexNode> conjunctions,
        List<IndexComponentFilter> filterDescriptors
    ) {
        // Collect filters and relevant expressions
        List<IndexFilter> filters = new ArrayList<>(filterDescriptors.size());
        List<QueryDataType> converterTypes = new ArrayList<>(filterDescriptors.size());
        Set<RexNode> exps = new HashSet<>();

        for (IndexComponentFilter filterDescriptor : filterDescriptors) {
            filters.add(filterDescriptor.getFilter());
            converterTypes.add(filterDescriptor.getConverterType());
            exps.addAll(filterDescriptor.getExpressions());
        }

        // Construct Calcite expressions.
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

        RexNode exp = RexUtil.composeConjunction(rexBuilder, exps);

        List<RexNode> remainderConjunctiveExps = excludeNodes(conjunctions, exps);
        RexNode remainderExp =
            remainderConjunctiveExps.isEmpty() ? null : RexUtil.composeConjunction(rexBuilder, remainderConjunctiveExps);

        // Prepare traits
        RelTraitSet traitSet = OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution);

        // Create the index scan
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.getHazelcastTable(scan);

        RelOptTable newRelTable = OptUtils.createRelTable(
            originalRelTable,
            originalHazelcastTable.withFilter(null),
            scan.getCluster().getTypeFactory()
        );

        IndexFilter filter = composeFilter(filters, index.getType(), index.getComponentsCount());

        if (filter == null) {
            return null;
        }

        return new MapIndexScanPhysicalRel(
            scan.getCluster(),
            traitSet,
            newRelTable,
            index,
            filter,
            converterTypes,
            exp,
            remainderExp
        );
    }

    public static RelNode createFullIndexScan(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        List<MapTableIndex> indexes
    ) {
        MapTableIndex firstIndex = null;

        for (MapTableIndex index : indexes) {
            if (isIndexSupported(index)) {
                firstIndex = index;

                break;
            }
        }

        if (firstIndex == null) {
            return null;
        }

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
            firstIndex,
            null,
            Collections.emptyList(),
            null,
            scanFilter
        );
    }

    /**
     * This method selects the best expression to be used as index filter from the list of candidates.
     *
     * @param type type of the index (SORTED, HASH)
     * @param candidates candidates that might be used as a filter
     * @param converterType expected converter type for the given component of the index
     * @return filter for the index component or {@code null} if no candidate could be applied
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private static IndexComponentFilter selectComponentFilter(
        IndexType type,
        List<IndexComponentCandidate> candidates,
        QueryDataType converterType
    ) {
        // First look for equality conditions, assuming that it is the most restrictive
        for (IndexComponentCandidate candidate : candidates) {
            if (candidate.getFilter() instanceof IndexEqualsFilter) {
                return new IndexComponentFilter(
                    candidate.getFilter(),
                    singletonList(candidate.getExpression()),
                    converterType
                );
            }
        }

        // Next look for IN, as it is worse than equality on a single value, but better than range
        for (IndexComponentCandidate candidate : candidates) {
            if (candidate.getFilter() instanceof IndexInFilter) {
                return new IndexComponentFilter(
                    candidate.getFilter(),
                    singletonList(candidate.getExpression()),
                    converterType
                );
            }
        }

        // Last, look for ranges
        if (type == IndexType.SORTED) {
            IndexFilterValue from = null;
            boolean fromInclusive = false;
            IndexFilterValue to = null;
            boolean toInclusive = false;
            List<RexNode> expressions = new ArrayList<>(2);

            for (IndexComponentCandidate candidate : candidates) {
                if (!(candidate.getFilter() instanceof IndexRangeFilter)) {
                    continue;
                }

                IndexRangeFilter candidateFilter = (IndexRangeFilter) candidate.getFilter();

                if (from == null && candidateFilter.getFrom() != null) {
                    from = candidateFilter.getFrom();
                    fromInclusive = candidateFilter.isFromInclusive();
                    expressions.add(candidate.getExpression());
                } else if (to == null && candidateFilter.getTo() != null) {
                    to = candidateFilter.getTo();
                    toInclusive = candidateFilter.isToInclusive();
                    expressions.add(candidate.getExpression());
                }
            }

            if (from != null || to != null) {
                IndexRangeFilter filter = new IndexRangeFilter(from, fromInclusive, to, toInclusive);

                return new IndexComponentFilter(filter, expressions, converterType);
            }
        }

        // Cannot create an index request for the given candidates
        return null;
    }

    /**
     * Composes the final filter from the list of single-column filters.
     *
     * @param filters single-column filters
     * @param indexType type of the index
     * @param indexComponentsCount number of components in the index
     * @return final filter or {@code null} if the filter could not be built for the given index type
     */
    private static IndexFilter composeFilter(List<IndexFilter> filters, IndexType indexType, int indexComponentsCount) {

        if (filters.size() == 1 && indexComponentsCount == 1) {
            IndexFilter res = filters.get(0);

            assert !(res instanceof IndexRangeFilter) || indexType == IndexType.SORTED;

            return res;
        } else {
            IndexFilter lastFilter = filters.get(filters.size() - 1);

            if (lastFilter instanceof IndexEqualsFilter) {
                return composeEqualsFilter(filters, (IndexEqualsFilter) lastFilter, indexType, indexComponentsCount);
            } else if (lastFilter instanceof IndexInFilter) {
                return composeInFilter(filters, (IndexInFilter) lastFilter, indexType, indexComponentsCount);
            } else {
                assert lastFilter instanceof IndexRangeFilter;

                assert indexType == IndexType.SORTED;

                return composeRangeFilter(filters, (IndexRangeFilter) lastFilter, indexComponentsCount);
            }
        }
    }

    /**
     * Composes an equality filter from multiple single-column components.
     * <p>
     * If the number of single-column filters is equal to the number of index components, the resulting filter is a composite
     * equality filter.
     * <p>
     * If the number of single-column filters is less than the number of index components, the resulting filter is a range
     * filter, with missing components filled with negative/positive infinities for the left and right bounds respectively.
     * <p>
     * If the range filter is required, and the target index type is not {@link IndexType#SORTED}, the result is {@code null}.
     *
     * @return composite filter or {@code null}
     */
    private static IndexFilter composeEqualsFilter(
        List<IndexFilter> filters,
        IndexEqualsFilter lastFilter,
        IndexType indexType,
        int indexComponentsCount
    ) {
        // Flatten all known values.
        List<Expression> components = new ArrayList<>(filters.size());
        List<Boolean> allowNulls = new ArrayList<>(filters.size());

        fillNonTerminalComponents(filters, components, allowNulls);

        components.addAll(lastFilter.getValue().getComponents());
        allowNulls.addAll(lastFilter.getValue().getAllowNulls());

        if (indexComponentsCount == components.size()) {
            // If there is a full match, then leave it as equals filter
            return new IndexEqualsFilter(new IndexFilterValue(components, allowNulls));
        } else {
            // Otherwise convert it to a range request
            if (indexType == IndexType.HASH) {
                return null;
            }

            List<Expression> fromComponents = components;
            List<Expression> toComponents = new ArrayList<>(components);

            List<Boolean> fromAllowNulls = allowNulls;
            List<Boolean> toAllowNulls = new ArrayList<>(fromAllowNulls);

            addInfiniteRanges(fromComponents, fromAllowNulls, true, toComponents, toAllowNulls, true, indexComponentsCount);

            return new IndexRangeFilter(
                new IndexFilterValue(fromComponents, fromAllowNulls),
                true,
                new IndexFilterValue(toComponents, toAllowNulls),
                true
            );
        }
    }

    /**
     * Create the final IN filter from the collection of per-column filters.
     * <p>
     * Consider the expression {@code {a=1 AND b IN (2,3)}}. After the conversion, the composite filter will be
     * {@code {a,b} IN {{1, 2}, {1, 3}}}.
     *
     * @param filters per-column filters
     * @param lastFilter the last IN filter
     * @param indexComponentsCount the number of index components
     * @return composite IN filter
     */
    // TODO: Currently IN filter loses collation!
    private static IndexFilter composeInFilter(
        List<IndexFilter> filters,
        IndexInFilter lastFilter,
        IndexType indexType,
        int indexComponentsCount
    ) {
        List<IndexFilter> newFilters = new ArrayList<>(lastFilter.getFilters().size());

        for (IndexFilter filter : lastFilter.getFilters()) {
            assert filter instanceof IndexEqualsFilter;

            IndexFilter newFilter = composeEqualsFilter(filters, (IndexEqualsFilter) filter, indexType, indexComponentsCount);

            if (newFilter == null) {
                // Cannot create a filter for one of the values of the IN clause. Stop.
                return null;
            }

            newFilters.add(newFilter);
        }

        return new IndexInFilter(newFilters);
    }

    /**
     * Create the composite range filter from the given per-column filters.
     * <p>
     * If there number of per-column filters if less than the number of index components, then infinite ranges are added
     * to the missing components.
     * <p>
     * Consider that we have two per-column filter as input: {@code {a=1}, {b>2 AND b<3}}.
     * <p>
     * If the index is defined as {@code {a, b}}, then the resulting filter would be {@code {a=1, b>2 AND a=1, b<3}}.
     * <p>
     * If the index is defined as {@code {a, b, c}}, then the resulting filter would be
     * {@code {a=1, b>2, c>NEGATIVE_INFINITY AND a=1, b<3, c<POSITIVE_INFINITY}}.
     *
     * @param filters all per-column filters
     * @param lastFilter the last filter (range)
     * @param componentsCount number of components in the filter
     * @return range filter
     */
    private static IndexFilter composeRangeFilter(List<IndexFilter> filters, IndexRangeFilter lastFilter, int componentsCount) {
        // Flatten non-terminal components.
        List<Expression> components = new ArrayList<>(filters.size());
        List<Boolean> allowNulls = new ArrayList<>();

        fillNonTerminalComponents(filters, components, allowNulls);

        // Add value of the current filter.
        List<Expression> fromComponents = components;
        List<Expression> toComponents = new ArrayList<>(components);

        List<Boolean> fromAllowNulls = allowNulls;
        List<Boolean> toAllowNulls = new ArrayList<>(fromAllowNulls);

        if (lastFilter.getFrom() != null) {
            fromComponents.add(lastFilter.getFrom().getComponents().get(0));
            fromAllowNulls.add(false);
        } else {
            if (componentsCount == 1) {
                fromComponents.add(ConstantExpression.create(NEGATIVE_INFINITY, QueryDataType.OBJECT));
                fromAllowNulls.add(false);
            } else {
                // In composite indexes null values are not stored separately. Therefore, we need to filter them out.
                fromComponents.add(ConstantExpression.create(null, QueryDataType.OBJECT));
                fromAllowNulls.add(true);
            }
        }

        if (lastFilter.getTo() != null) {
            toComponents.add(lastFilter.getTo().getComponents().get(0));
        } else {
            toComponents.add(ConstantExpression.create(POSITIVE_INFINITY, QueryDataType.OBJECT));
        }

        toAllowNulls.add(false);

        // Fill missing part of the range request.
        addInfiniteRanges(
            fromComponents,
            fromAllowNulls,
            lastFilter.isFromInclusive(),
            toComponents,
            toAllowNulls,
            lastFilter.isToInclusive(),
            componentsCount
        );

        return new IndexRangeFilter(
            new IndexFilterValue(fromComponents, fromAllowNulls),
            lastFilter.isFromInclusive(),
            new IndexFilterValue(toComponents, toAllowNulls),
            lastFilter.isToInclusive()
        );
    }

    /**
     * Given the list of column filters, flatten their expressions and allow-null flags.
     * <p>
     * The operation is performed for all filters except for the last one, because treatment of the last filter might differ
     * depending on the total number of components in the index.
     *
     * @param filters column filters
     * @param components expressions that would form the final filter
     * @param allowNulls allow-null collection relevant to components
     */
    private static void fillNonTerminalComponents(
        List<IndexFilter> filters,
        List<Expression> components,
        List<Boolean> allowNulls
    ) {
        for (int i = 0; i < filters.size() - 1; i++) {
            IndexEqualsFilter filter0 = (IndexEqualsFilter) filters.get(i);

            IndexFilterValue value = filter0.getValue();

            assert value.getComponents().size() == 1;

            components.add(value.getComponents().get(0));
            allowNulls.add(value.getAllowNulls().get(0));
        }

        assert components.size() == filters.size() - 1;
        assert allowNulls.size() == filters.size() - 1;
    }

    /**
     * Adds infinite ranges to filter components, to match the number of index components.
     * <p>
     * For example, for the index on {@code (a, b)} and the expression {@code a=1}, the original filter would be
     * {@code from={a=1}, to={a=1}}.
     * <p>
     * Since the index has two components, we need to add infinite ranges to components that do not have explicit filters.
     * After the method finishes, the filter would be {@code from={a=1, b=NEGATIVE_INFINITY}, to={a=1, b=POSITIVE_INFINITY}}.
     * <p>
     * The combination of ranges also depends on the inclusion. For example, {@code {a>1}} yields {@code NEGATIVE_INFINITY}
     * for {@code b} on the left side, while {@code {a>=1}} yields {@code POSITIVE_INFINITY}.
     *
     * @param fromComponents expressions for the "from" part
     * @param toComponents expressions for the "to" part
     * @param componentsCount the number of components in the index
     */
    private static void addInfiniteRanges(
        List<Expression> fromComponents,
        List<Boolean> fromAllowNulls,
        boolean fromInclusive,
        List<Expression> toComponents,
        List<Boolean> toAllowNulls,
        boolean toInclusive,
        int componentsCount
    ) {
        int count = componentsCount - fromComponents.size();

        ComparableIdentifiedDataSerializable leftBound = fromInclusive ? NEGATIVE_INFINITY : POSITIVE_INFINITY;
        ComparableIdentifiedDataSerializable toBound = toInclusive ? POSITIVE_INFINITY : NEGATIVE_INFINITY;

        for (int i = 0; i < count; i++) {
            fromComponents.add(ConstantExpression.create(leftBound, QueryDataType.OBJECT));
            toComponents.add(ConstantExpression.create(toBound, QueryDataType.OBJECT));

            fromAllowNulls.add(false);
            toAllowNulls.add(false);
        }
    }

    /**
     * Convert the comparison operator to the opposite operator. E.g. {@code >} becomes {@code <}.
     * <p>
     * This method is invoked during expression normalization to simplify further processing. E.g. {@code ? < a} is converted
     * into {@code a > ?}
     *
     * @param kind original operator
     * @return the opposite operator
     */
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
     * Checks whether the index could be used by the engine.
     * <p>
     * At the moment only SORTED and HASH indexes could be used.
     *
     * @param index the index
     * @return {@code true} if the index could be used, {@code false} otherwise
     */
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

        RexNode normalizedOperand1 = removeCastIfPossible(operand1, false);
        RexNode normalizedOperand2 = removeCastIfPossible(operand2, false);

        return BiTuple.of(normalizedOperand1, normalizedOperand2);
    }

    /**
     * Removes CAST operator from the column expression when possible.
     * <p>
     * The following expression might be found: {@code CAST(a AS BIGINT) > ?}. This may happen either due to type coercion
     * during sql->rel conversion, or due to explicit user request. In the general case, a function applied to the column makes
     * usage of the index on that column impossible.
     * <p>
     * In case of the {@code CAST} operator we may try to remove the CAST, thus relying on internal index converters to
     * downcast the other side of the comparison expression. See {@link TypeConverters}.
     *
     * @param node original node, possibly CAST
     * @param force whether to remove CAST forcefully even for conversion that is otherwise invalid wrt the storage (used for
     *              {@code IS NULL} operator)
     * @return original node if there is nothing to unwrap, or the operand of the CAST
     */
    private static RexNode removeCastIfPossible(RexNode node, boolean force) {
        if (node.getKind() == SqlKind.CAST) {
            RexCall node0 = (RexCall) node;

            RexNode from = node0.getOperands().get(0);

            if (from instanceof RexInputRef) {
                RelDataType fromType = from.getType();
                RelDataType toType = node0.getType();

                if (force) {
                    // Forced unwrap for IS NULL expression
                    return from;
                }

                if (fromType.equals(toType)) {
                    // Redundant conversion => unwrap
                    return from;
                }

                if (fromType.getSqlTypeName().getFamily() == NUMERIC && toType.getSqlTypeName().getFamily() == NUMERIC) {
                    // Converting between numeric types => unwrap
                    return from;
                }
            }
        }

        return node;
    }

    private static List<RexNode> excludeNodes(Collection<RexNode> nodes, Set<RexNode> exclusions) {
        List<RexNode> res = new ArrayList<>(nodes.size());

        for (RexNode node : nodes) {
            if (exclusions.contains(node)) {
                continue;
            }

            res.add(node);
        }

        return res;
    }

    /**
     * Create collation trait for the given scan and index.
     *
     * @param scan  Scan.
     * @param index Index.
     * @return Collation trait.
     */
    // TODO: Proper collation integration for indexes! Must be applied to all places where the MapIndexScanPhysicalRel is created.
    @SuppressWarnings("unused")
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

    /**
     * Specialized field type provider that do not expect any fields.
     */
    private static final class FieldTypeProvider implements PlanNodeFieldTypeProvider {

        private static final FieldTypeProvider INSTANCE = new FieldTypeProvider();

        private FieldTypeProvider() {
            // No-op.
        }

        @Override
        public QueryDataType getType(int index) {
            throw new IllegalStateException("The operation should not be called.");
        }
    }
}
