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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
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

/**
 * Helper class to resolve indexes.
 */
@SuppressWarnings("rawtypes")
public final class IndexResolver {
    private IndexResolver() {
        // No-op.
    }

    /**
     * The main entry point for index planning.
     * <p>
     * Analyzes the filter of the input scan operator, and produces zero, one or more {@link MapIndexScanPhysicalRel}
     * operators.
     *
     * @param scan scan operator to be analyzed
     * @param distribution distribution that will be passed to created index scan rels
     * @param indexes indexes available on the map being scanned
     * @return zero, one or more index scan rels
     */
    public static List<RelNode> createIndexScans(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        List<MapTableIndex> indexes
    ) {
        // If there is no filter, no index can help speed up the query, return.
        RexNode filter = scan.getTableUnwrapped().getFilter();

        if (filter == null) {
            return Collections.emptyList();
        }

        // Filter out unsupported indexes. Only SORTED and HASH indexes are supported.
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

        // Convert expression into CNF. Examples:
        // - {a=1 AND b=2} is converted into {a=1}, {b=2}
        // - {a=1 OR b=2} is unchanged
        List<RexNode> conjunctions = createConjunctiveFilter(filter);

        // Create a map from a column to a list of expressions that could be used by indexes.
        // For example, for the expression {a>1 AND a<3 AND b=5 AND c>d}, three candidates will be created:
        // a -> {>1}, {<3}
        // b -> {=5}
        Map<Integer, List<IndexComponentCandidate>> candidates = prepareSingleColumnCandidates(
            conjunctions,
            OptUtils.getCluster(scan).getParameterMetadata(),
            allIndexedFieldOrdinals
        );

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        List<RelNode> rels = new ArrayList<>(supportedIndexes.size());

        for (MapTableIndex index : supportedIndexes) {
            // Create index scan based on candidates, if possible. Candidates could be merged into more complex
            // filters whenever possible.
            RelNode rel = createIndexScan(scan, distribution, index, conjunctions, candidates);

            if (rel != null) {
                rels.add(rel);
            }
        }

        return rels;
    }

    /**
     * Decompose the original scan filter into CNF components.
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
     * @param expressions                   CNF nodes
     * @param allIndexedFieldOrdinals ordinals of all columns that have some indexes. Helps to filter out candidates that
     *                                definitely cannot be used earlier.
     */
    private static Map<Integer, List<IndexComponentCandidate>> prepareSingleColumnCandidates(
        List<RexNode> expressions,
        QueryParameterMetadata parameterMetadata,
        Set<Integer> allIndexedFieldOrdinals
    ) {
        Map<Integer, List<IndexComponentCandidate>> res = new HashMap<>();

        // Iterate over each CNF component of the expression.
        for (RexNode expression : expressions) {
            // Try creating a candidate for the expression. The candidate is created iff the expression could be used
            // by some index implementation (SORTED, HASH)
            IndexComponentCandidate candidate = prepareSingleColumnCandidate(expression, parameterMetadata);

            if (candidate == null) {
                // Expression cannot be used by any index implementation, skip
                continue;
            }

            if (!allIndexedFieldOrdinals.contains(candidate.getColumnIndex())) {
                // Expression could be used by some index implementation, but the map doesn't have indexes using this column
                // Therefore, the expression could not be used, skip
                continue;
            }

            // Group candidates by column. E.g. {a>1 AND a<3} is grouped into a single map entry: a->{>1},{<3}
            res.computeIfAbsent(candidate.getColumnIndex(), (k) -> new ArrayList<>()).add(candidate);
        }

        return res;
    }

    /**
     * Prepares an expression candidate for the given RexNode.
     * <p>
     * See method body for the details of supported expressions.
     *
     * @param exp expression
     * @return candidate or {@code null} if the expression cannot be used by any index implementation
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static IndexComponentCandidate prepareSingleColumnCandidate(
        RexNode exp,
        QueryParameterMetadata parameterMetadata
    ) {
        SqlKind kind = exp.getKind();

        switch (kind) {
            case IS_TRUE:
            case IS_FALSE:
            case IS_NOT_TRUE:
            case IS_NOT_FALSE:
                // Special case for boolean columns: SELECT * FROM t WHERE f_boolean IS (NOT) TRUE/FALSE.
                // Given that boolean column have only 3 values (true, false, null), any such expression could be converted
                // to either EQUALS or IN predicates. Examples:
                // {f_boolean IS TRUE} -> EQUALS(TRUE)
                // {f_boolean IS NOT TRUE} -> IN(EQUALS(FALSE), EQUALS(NULL))
                return prepareSingleColumnCandidateBooleanIsTrueFalse(
                    exp,
                    removeCastIfPossible(((RexCall) exp).getOperands().get(0)),
                    kind
                );

            case INPUT_REF:
                // Special case for boolean columns: SELECT * FROM t WHERE f_boolean
                // Equivalent to SELECT * FROM t WHERE f_boolean IS TRUE
                return prepareSingleColumnCandidateBooleanIsTrueFalse(
                    exp,
                    exp,
                    SqlKind.IS_TRUE
                );

            case NOT:
                // Special case for boolean columns: SELECT * FROM t WHERE NOT f_boolean
                // Equivalent to SELECT * FROM t WHERE f_boolean IS FALSE
                return prepareSingleColumnCandidateBooleanIsTrueFalse(
                    exp,
                    removeCastIfPossible(((RexCall) exp).getOperands().get(0)),
                    SqlKind.IS_FALSE
                );

            case IS_NULL:
                // Handle SELECT * FROM WHERE column IS NULL.
                // Internally it is converted into EQUALS(NULL) filter
                return prepareSingleColumnCandidateIsNull(
                    exp,
                    removeCastIfPossible(((RexCall) exp).getOperands().get(0))
                );

            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case EQUALS:
                // Handle comparison predicates.
                // Converted to either EQUALS or RANGE filter
                BiTuple<RexNode, RexNode> operands = extractComparisonOperands(exp);

                return prepareSingleColumnCandidateComparison(
                    exp,
                    kind,
                    operands.element1(),
                    operands.element2(),
                    parameterMetadata
                );

            case OR:
                // Handle OR/IN predicates. If OR condition refer to only a single column and all comparisons are equality
                // comparisons, then IN filter is created. Otherwise null is returned. Examples:
                // {a=1 OR a=2} -> IN(EQUALS(1), EQUALS(2))
                // {a=1 OR a>2} -> null
                // {a=1 OR b=2} -> null
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
     * such expression could be converted to equivalent equals or IN predicate:
     * - IS TRUE -> EQUALS(TRUE)
     * - IS FALSE -> EQUALS(FALSE)
     * - IS NOT TRUE -> IN(EQUALS(FALSE), EQUALS(NULL))
     * - IS NOT FALSE -> IN(EQUALS(TRUE), EQUALS(NULL))
     *
     * @param exp original expression, e.g. {col IS TRUE}
     * @param operand  operand, e.g. {col}; CAST must be unwrapped before the method is invoked
     * @param kind expression type
     * @return candidate or {@code null}
     */
    private static IndexComponentCandidate prepareSingleColumnCandidateBooleanIsTrueFalse(
        RexNode exp,
        RexNode operand,
        SqlKind kind
    ) {
        if (operand.getKind() != SqlKind.INPUT_REF) {
            // The operand is not a column, e.g. {'true' IS TRUE}, index cannot be used
            return null;
        }

        if (operand.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
            // The column is not of BOOLEAN type. We should never hit this branch normally. Added here only for safety.
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

    /**
     * Try creating a candidate filter for the "IS NULL" expression.
     * <p>
     * Returns the filter EQUALS(null) with "allowNulls-true".
     *
     * @param exp original expression, e.g. {col IS NULL}
     * @param operand operand, e.g. {col}; CAST must be unwrapped before the method is invoked
     * @return candidate or {@code null}
     */
    private static IndexComponentCandidate prepareSingleColumnCandidateIsNull(RexNode exp, RexNode operand) {
        if (operand.getKind() != SqlKind.INPUT_REF) {
            // The operand is not a column, e.g. {'literal' IS NULL}, index cannot be used
            return null;
        }

        int columnIndex = ((RexInputRef) operand).getIndex();

        QueryDataType type = SqlToQueryType.map(operand.getType().getSqlTypeName());

        // Create a value with "allowNulls=true"
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

    /**
     * Try creating a candidate filter for comparison operator.
     *
     * @param exp the original expression
     * @param kind expression kine (=, >, <, >=, <=)
     * @param operand1 the first operand (CAST must be unwrapped before the method is invoked)
     * @param operand2 the second operand (CAST must be unwrapped before the method is invoked)
     * @param parameterMetadata parameter metadata for expressions like {a>?}
     * @return candidate or {@code null}
     */
    private static IndexComponentCandidate prepareSingleColumnCandidateComparison(
        RexNode exp,
        SqlKind kind,
        RexNode operand1,
        RexNode operand2,
        QueryParameterMetadata parameterMetadata
    ) {
        // Normalize operand positions, so that the column is always goes first.
        // The condition (kind) is changed accordingly (e.g. ">" to "<").
        if (operand1.getKind() != SqlKind.INPUT_REF && operand2.getKind() == SqlKind.INPUT_REF) {
            kind = inverseIndexConditionKind(kind);

            RexNode tmp = operand1;
            operand1 = operand2;
            operand2 = tmp;
        }

        if (operand1.getKind() != SqlKind.INPUT_REF) {
            // No columns in the expression, index cannot be used. E.g. {'a' > 'b'}
            return null;
        }

        int columnIndex = ((RexInputRef) operand1).getIndex();

        if (!IndexRexVisitor.isValid(operand2)) {
            // The second operand cannot be used for index filter because its value possibly row-dependent.
            // E.g. {column_a > column_b}.
            return null;
        }

        // Convert the second operand into Hazelcast expression. The expression will be evaluated once before index scan is
        // initiated, to construct the proper filter for index lookup.
        Expression<?> filterValue = convertToExpression(operand2, parameterMetadata);

        if (filterValue == null) {
            // The second operand cannot be converted to expression. Do not throw an exception here, just do not use the faulty
            // condition for index. The proper exception will be thrown on later stages when attempting to convert Calcite rel
            // tree to Hazelcast plan.
            return null;
        }

        // Create the value that will be passed to filters. Not that "allowNulls=false" here, because any NULL in the comparison
        // operator never returns "TRUE" and hence always returns an empty result set.
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

    /**
     * Prepare candidate for OR expression if possible.
     * <p>
     * We support only equality conditions on the same columns. Ranges and conditions on different columns (aka "index joins")
     * are not supported.
     *
     * @param exp the OR expression
     * @param nodes components of the OR expression
     * @param parameterMetadata parameter metadata
     * @return candidate or {code null}
     */
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
                // The component of the OR expression cannot be used by any index implementation
                return null;
            }

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

            // Flatten. E.g. ((a=1 OR a=2) OR a=3) is parsed into IN(1, 2) and OR(3), that is then flatten into IN(1, 2, 3)
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
     * @param scan the original map scan
     * @param distribution the original map distribution
     * @param index index to be considered
     * @param conjunctions CNF components of the original map filter
     * @param candidates resolved candidates
     * @return index scan or {@code null}.
     */
    public static RelNode createIndexScan(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        MapTableIndex index,
        List<RexNode> conjunctions,
        Map<Integer, List<IndexComponentCandidate>> candidates
    ) {
        List<IndexComponentFilter> filters = new ArrayList<>(index.getFieldOrdinals().size());

        // Iterate over every index component from the beginning and try to form a filter to it
        for (int i = 0; i < index.getFieldOrdinals().size(); i++) {
            int fieldOrdinal = index.getFieldOrdinals().get(i);
            QueryDataType fieldConverterType = index.getFieldConverterTypes().get(i);

            List<IndexComponentCandidate> fieldCandidates = candidates.get(fieldOrdinal);

            if (fieldCandidates == null) {
                // No candidates available for the given component, stop.
                // Consider the index {a, b, c}. If there is a condition "WHERE a=1 AND c=3", only "a=1" could be
                // used for index filter.
                break;
            }

            // Create the filter for the given index component if possible.
            // Separate candidates are possible merged into a single complex filter at this stage.
            // Consider the index {a}, and the condition "WHERE a>1 AND a<5". In this case two distinct range candidates
            // {>1} and {<5} are combined into a single RANGE filter {>1 AND <5}
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
                // For composite indexes, non-equals condition must always be the last part of the request, otherwise we stop.
                // Consider an index SORTED{a,b} and the condition "WHERE a>1 AND b=1". We cannot use both {a>1} and {b=1}
                // filters here, because there is no single index request that could return such entries. Therefore, we use only
                // {a>1} filter.
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

        // Prepare table
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.getHazelcastTable(scan);

        RelOptTable newRelTable = OptUtils.createRelTable(
            originalRelTable,
            originalHazelcastTable.withFilter(null),
            scan.getCluster().getTypeFactory()
        );

        // Try composing the final filter out of the isolated component filters if possible
        IndexFilter filter = composeFilter(filters, index.getType(), index.getComponentsCount());

        if (filter == null) {
            return null;
        }

        // Construct the scan
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

    /**
     * Create an index scan without any filter. Used by HD maps only.
     *
     * @param scan the original scan operator
     * @param distribution the original distribution
     * @param indexes available indexes
     * @return index scan or {@code null}
     */
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
        assert !filters.isEmpty();

        if (indexComponentsCount == 1) {
            // Non-composite index. Just pick the first filter
            assert filters.size() == 1;

            IndexFilter res = filters.get(0);

            assert !(res instanceof IndexRangeFilter) || indexType == IndexType.SORTED;

            return res;
        } else {
            // At this point component filters has the form "1=EQUALS, 2=EQUALS, ..., N=EQUALS/RANGE/IN".
            // Compose the final filter based on the type of the last resolved filter.
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
     * <p>
     * Examples:
     * <ul>
     *     <li>SORTED(a, b), {a=1, b=2} => EQUALS(1), EQUALS(2) </li>
     *     <li>HASH(a, b), {a=1, b=2} => EQUALS(1), EQUALS(2) </li>
     *     <li>SORTED(a, b), {a=1} => EQUALS(1), RANGE(INF) </li>
     *     <li>HASH(a, b), {a=1} => null </li>
     * </ul>
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

        RexNode normalizedOperand1 = removeCastIfPossible(operand1);
        RexNode normalizedOperand2 = removeCastIfPossible(operand2);

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
     * @return original node if there is nothing to unwrap, or the operand of the CAST
     */
    @SuppressWarnings("checkstyle:NestedIfDepth")
    private static RexNode removeCastIfPossible(RexNode node) {
        if (node.getKind() == SqlKind.CAST) {
            RexCall node0 = (RexCall) node;

            RexNode from = node0.getOperands().get(0);

            if (from instanceof RexInputRef) {
                RelDataType fromType = from.getType();
                RelDataType toType = node0.getType();

                if (fromType.equals(toType)) {
                    // Redundant conversion => unwrap
                    return from;
                }

                QueryDataTypeFamily fromFamily = SqlToQueryType.map(fromType.getSqlTypeName()).getTypeFamily();
                QueryDataTypeFamily toFamily = SqlToQueryType.map(toType.getSqlTypeName()).getTypeFamily();

                if (QueryDataTypeUtils.isNumeric(fromFamily) && QueryDataTypeUtils.isNumeric(toFamily)) {
                    // Converting between numeric types
                    if (toFamily.getPrecedence() > fromFamily.getPrecedence()) {
                        // Only conversion from smaller type to bigger type could be removed safely. Otherwise the
                        // overflow error is possible, so we cannot remove the CAST.
                        return from;
                    }
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
