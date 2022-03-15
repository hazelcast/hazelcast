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

package com.hazelcast.jet.sql.impl.opt.physical.index;

import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.hazelcast.config.IndexType;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpression;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import com.hazelcast.query.impl.ComparableIdentifiedDataSerializable;
import com.hazelcast.query.impl.TypeConverters;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.hazelcast.config.IndexType.HASH;
import static com.hazelcast.config.IndexType.SORTED;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.createRelTable;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.getCluster;
import static com.hazelcast.query.impl.CompositeValue.NEGATIVE_INFINITY;
import static com.hazelcast.query.impl.CompositeValue.POSITIVE_INFINITY;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.Direction.DESCENDING;

/**
 * Helper class to resolve indexes.
 */
@SuppressWarnings({"rawtypes", "checkstyle:MethodCount"})
public final class IndexResolver {
    private IndexResolver() {
        // No-op.
    }

    /**
     * The main entry point for index planning.
     * <p>
     * Analyzes the filter of the input scan operator, and produces zero, one or more {@link IndexScanMapPhysicalRel}
     * operators.
     * <p>
     * First, the full index scans are created and the covered (prefix-based) scans are excluded.
     * Second, the lookups are created and if lookup's collation is equal to the full scan's collation,
     * the latter one is excluded.
     *
     * @param scan    scan operator to be analyzed
     * @param indexes indexes available on the map being scanned
     * @return zero, one or more index scan rels
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity", "checkstyle:MethodLength"})
    public static Collection<RelNode> createIndexScans(FullScanLogicalRel scan, List<MapTableIndex> indexes) {
        RexNode filter = OptUtils.extractHazelcastTable(scan).getFilter();

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

        List<RelNode> fullScanRels = new ArrayList<>(supportedIndexes.size());

        // There is no filter, still generate index scans for
        // possible ORDER BY clause on the upper level
        for (MapTableIndex index : supportedIndexes) {

            if (index.getType() == SORTED) {
                // Only for SORTED index create full index scans that might be potentially
                // utilized by sorting operator.
                List<Boolean> ascs = buildFieldDirections(index, true);
                RelNode relAscending = createFullIndexScan(scan, index, ascs, true);

                if (relAscending != null) {
                    fullScanRels.add(relAscending);
                    RelNode relDescending = replaceCollationDirection(relAscending, DESCENDING);
                    fullScanRels.add(relDescending);
                }
            }
        }

        Map<RelCollation, RelNode> fullScanRelsMap = excludeCoveredCollations(fullScanRels);
        if (filter == null) {
            // Exclude prefix-based covered index scans
            return fullScanRelsMap.values();
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
                getCluster(scan).getParameterMetadata(),
                allIndexedFieldOrdinals
        );

        if (candidates.isEmpty()) {
            return fullScanRelsMap.values();
        }

        List<RelNode> rels = new ArrayList<>(supportedIndexes.size());
        for (MapTableIndex index : supportedIndexes) {
            // Create index scan based on candidates, if possible. Candidates could be merged into more complex
            // filters whenever possible.
            List<Boolean> ascs = buildFieldDirections(index, true);
            RelNode relAscending = createIndexScan(scan, index, conjunctions, candidates, ascs);

            if (relAscending != null) {
                RelCollation relAscCollation = getCollation(relAscending);
                // Exclude a full scan that has the same collation
                fullScanRelsMap.remove(relAscCollation);

                rels.add(relAscending);

                if (relAscCollation.getFieldCollations().size() > 0) {
                    RelNode relDescending = replaceCollationDirection(relAscending, DESCENDING);
                    rels.add(relDescending);

                    RelCollation relDescCollation = getCollation(relDescending);
                    // Exclude a full scan that has the same collation
                    fullScanRelsMap.remove(relDescCollation);
                }
            }
        }

        rels.addAll(fullScanRelsMap.values());
        return rels;
    }

    private static RelCollation getCollation(RelNode rel) {
        return rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
    }

    /**
     * Replaces a direction in the collation trait of the rel
     *
     * @param rel       the rel
     * @param direction the collation
     * @return the rel with changed collation
     */
    private static RelNode replaceCollationDirection(RelNode rel, Direction direction) {
        RelCollation collation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        List<RelFieldCollation> newFields = new ArrayList<>(collation.getFieldCollations().size());
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            RelFieldCollation newFieldCollation = new RelFieldCollation(fieldCollation.getFieldIndex(), direction);
            newFields.add(newFieldCollation);
        }

        RelCollation newCollation = RelCollations.of(newFields);
        RelTraitSet traitSet = rel.getTraitSet();
        traitSet = OptUtils.traitPlus(traitSet, newCollation);

        return rel.copy(traitSet, rel.getInputs());
    }

    /**
     * Filters out index scans which collation is covered (prefix based) by another index scan in the rels.
     *
     * @param rels the list of index scans
     * @return a filtered out map of collation to rel
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    private static Map<RelCollation, RelNode> excludeCoveredCollations(List<RelNode> rels) {
        // Order the index scans based on their collation
        TreeMap<RelCollation, RelNode> relsTreeMap = new TreeMap<>(RelCollationComparator.INSTANCE);

        // Put the rels into the ordered TreeMap
        for (RelNode rel : rels) {
            relsTreeMap.put(rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE), rel);
        }

        Map<RelCollation, RelNode> resultMap = new HashMap<>();
        Map.Entry<RelCollation, RelNode> prevEntry = null;
        // Go through the ordered collations and exclude covered ones
        for (Map.Entry<RelCollation, RelNode> entry : relsTreeMap.descendingMap().entrySet()) {
            RelCollation collation = entry.getKey();
            RelNode relNode = entry.getValue();

            if (prevEntry == null) {
                resultMap.put(collation, relNode);
                prevEntry = entry;
            } else {
                RelCollation prevCollation = prevEntry.getKey();
                if (!prevCollation.satisfies(collation)) {
                    prevEntry = entry;
                    resultMap.put(collation, relNode);
                }
            }
        }
        return resultMap;
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
     * @param expressions             CNF nodes
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
            res.computeIfAbsent(candidate.getColumnIndex(), (k) -> new ArrayList<>())
                    .add(candidate);
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
            case SEARCH:
                operands = extractComparisonOperands(exp);

                return prepareSingleColumnSearchCandidateComparison(
                        exp,
                        operands.element1(),
                        operands.element2()
                );
            case OR:
                // Handle OR/IN predicates. If OR condition refer to only a single column and all comparisons are equality
                // comparisons, then composite filter is created. Otherwise null is returned. Examples:
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
     * @param exp     original expression, e.g. {col IS TRUE}
     * @param operand operand, e.g. {col}; CAST must be unwrapped before the method is invoked
     * @param kind    expression type
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
                filter = new IndexCompositeFilter(
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

                filter = new IndexCompositeFilter(
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
     * @param exp     original expression, e.g. {col IS NULL}
     * @param operand operand, e.g. {col}; CAST must be unwrapped before the method is invoked
     * @return candidate or {@code null}
     */
    private static IndexComponentCandidate prepareSingleColumnCandidateIsNull(RexNode exp, RexNode operand) {
        if (operand.getKind() != SqlKind.INPUT_REF) {
            // The operand is not a column, e.g. {'literal' IS NULL}, index cannot be used
            return null;
        }

        int columnIndex = ((RexInputRef) operand).getIndex();

        QueryDataType type = HazelcastTypeUtils.toHazelcastType(operand.getType());

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
     * @param exp               the original expression
     * @param kind              expression kine (=, >, <, >=, <=)
     * @param operand1          the first operand (CAST must be unwrapped before the method is invoked)
     * @param operand2          the second operand (CAST must be unwrapped before the method is invoked)
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

    @SuppressWarnings({"ConstantConditions", "UnstableApiUsage"})
    private static IndexComponentCandidate prepareSingleColumnSearchCandidateComparison(
            RexNode exp,
            RexNode operand1,
            RexNode operand2
    ) {
        // SARG is supported only for literals, not for dynamic parameters
        if (operand1.getKind() != SqlKind.INPUT_REF || operand2.getKind() != SqlKind.LITERAL) {
            return null;
        }
        int columnIndex = ((RexInputRef) operand1).getIndex();
        RexLiteral literal = (RexLiteral) operand2;

        QueryDataType hazelcastType = HazelcastTypeUtils.toHazelcastType(literal.getType());

        RangeSet<?> rangeSet = RexToExpression.extractRangeFromSearch(literal);
        if (rangeSet == null) {
            return null;
        }

        Set<? extends Range<?>> ranges = rangeSet.asRanges();

        IndexFilter indexFilter;
        if (ranges.size() == 1) {
            indexFilter = createIndexFilterForSingleRange(Iterables.getFirst(ranges, null), hazelcastType);
        } else {
            indexFilter = new IndexCompositeFilter(
                    toList(ranges, range -> createIndexFilterForSingleRange(range, hazelcastType)));
        }

        return new IndexComponentCandidate(exp, columnIndex, indexFilter);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private static IndexFilter createIndexFilterForSingleRange(Range<?> range, QueryDataType hazelcastType) {
        IndexFilterValue lowerBound0 = null;

        // Range doesn't have to have both bounds.
        if (range.hasLowerBound()) {
            Expression<?> lowerBound = ConstantExpression.create(range.lowerEndpoint(), hazelcastType);
            lowerBound0 = new IndexFilterValue(
                    singletonList(lowerBound),
                    singletonList(false)
            );
            if (isSingletonRange(range)) {
                return new IndexEqualsFilter(lowerBound0);
            }
        }

        if (range.hasUpperBound()) {
            Expression<?> upperBound = ConstantExpression.create(range.upperEndpoint(), hazelcastType);
            IndexFilterValue upperBound0 = new IndexFilterValue(
                    singletonList(upperBound),
                    singletonList(false)
            );

            if (lowerBound0 == null) {
                return new IndexRangeFilter(null, false, upperBound0, range.upperBoundType() == BoundType.CLOSED);
            }

            return new IndexRangeFilter(lowerBound0, range.lowerBoundType() == BoundType.CLOSED,
                    upperBound0, range.upperBoundType() == BoundType.CLOSED);
        } else {
            assert lowerBound0 != null;

            return new IndexRangeFilter(lowerBound0, range.lowerBoundType() == BoundType.CLOSED, null, false);
        }
    }

    private static <T extends Comparable<T>> boolean isSingletonRange(Range<T> range) {
        return range.hasLowerBound() && range.hasUpperBound()
                && range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint().compareTo(range.upperEndpoint()) == 0;
    }

    private static Expression<?> convertToExpression(RexNode operand, QueryParameterMetadata parameterMetadata) {
        try {
            RexToExpressionVisitor visitor =
                    new RexToExpressionVisitor(PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER, parameterMetadata);

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
     * @param exp               the OR expression
     * @param nodes             components of the OR expression
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

            if (!(candidateFilter instanceof IndexEqualsFilter || candidateFilter instanceof IndexCompositeFilter)) {
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
                filters.addAll(((IndexCompositeFilter) candidateFilter).getFilters());
            }
        }

        assert columnIndex != null;

        IndexCompositeFilter inFilter = new IndexCompositeFilter(filters);

        return new IndexComponentCandidate(
                exp,
                columnIndex,
                inFilter
        );
    }

    /**
     * Create index scan for the given index if possible.
     *
     * @param scan         the original map scan
     * @param index        index to be considered
     * @param conjunctions CNF components of the original map filter
     * @param candidates   resolved candidates
     * @param ascs         a list of index field collations
     * @return index scan or {@code null}.
     */
    public static RelNode createIndexScan(
            FullScanLogicalRel scan,
            MapTableIndex index,
            List<RexNode> conjunctions,
            Map<Integer, List<IndexComponentCandidate>> candidates,
            List<Boolean> ascs
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
            // Separate candidates are possibly merged into a single complex filter at this stage.
            // Consider the index {a}, and the condition "WHERE a>1 AND a<5". In this case two distinct range candidates
            // {>1} and {<5} are combined into a single RANGE filter {>1 AND <5}
            IndexComponentFilter filter = IndexComponentFilterResolver.findBestComponentFilter(
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
        return createIndexScan(scan, index, conjunctions, filters, ascs);
    }

    private static IndexScanMapPhysicalRel createIndexScan(
            FullScanLogicalRel scan,
            MapTableIndex index,
            List<RexNode> conjunctions,
            List<IndexComponentFilter> filterDescriptors,
            List<Boolean> ascs
    ) {
        // Collect filters and relevant expressions
        List<IndexFilter> filters = new ArrayList<>(filterDescriptors.size());
        Set<RexNode> exps = new HashSet<>();

        for (IndexComponentFilter filterDescriptor : filterDescriptors) {
            filters.add(filterDescriptor.getFilter());
            exps.addAll(filterDescriptor.getExpressions());
        }

        // Construct Calcite expressions.
        RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

        RexNode exp = RexUtil.composeConjunction(rexBuilder, exps);

        List<RexNode> remainderConjunctiveExps = excludeNodes(conjunctions, exps);
        RexNode remainderExp =
                remainderConjunctiveExps.isEmpty() ? null : RexUtil.composeConjunction(rexBuilder, remainderConjunctiveExps);

        // Prepare traits
        RelTraitSet traitSet = scan.getTraitSet();

        // Make a collation trait
        RelCollation relCollation = buildCollationTrait(scan, index, ascs);
        traitSet = OptUtils.traitPlus(traitSet, relCollation);

        // Prepare table
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.extractHazelcastTable(scan);

        RelOptTable newRelTable = createRelTable(
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
        return new IndexScanMapPhysicalRel(
                scan.getCluster(),
                OptUtils.toPhysicalConvention(traitSet),
                newRelTable,
                index,
                filter,
                exp,
                remainderExp
        );
    }

    /**
     * Builds a collation with collation fields re-mapped according to the table projections.
     *
     * @param scan  the logical map scan
     * @param index the index
     * @param ascs  the collation of index fields
     * @return the new collation trait
     */
    private static RelCollation buildCollationTrait(
            FullScanLogicalRel scan,
            MapTableIndex index,
            List<Boolean> ascs
    ) {
        if (index.getType() != SORTED) {
            return RelCollations.of(Collections.emptyList());
        }
        List<RelFieldCollation> fields = new ArrayList<>(index.getFieldOrdinals().size());
        HazelcastTable table = OptUtils.extractHazelcastTable(scan);
        // Extract those projections that are direct input field references. Only those can be used
        // for index access
        List<Integer> fieldProjects = table.getProjects()
                .stream().filter(expr -> expr instanceof RexInputRef)
                .map(inputRef -> ((RexInputRef) inputRef).getIndex())
                .collect(Collectors.toList());

        for (int i = 0; i < index.getFieldOrdinals().size(); ++i) {
            Integer indexFieldOrdinal = index.getFieldOrdinals().get(i);

            int remappedIndexFieldOrdinal = fieldProjects.indexOf(indexFieldOrdinal);
            if (remappedIndexFieldOrdinal == -1) {
                // The field is not used in the query
                break;
            }
            Direction direction = ascs.get(i) ? ASCENDING : DESCENDING;
            RelFieldCollation fieldCollation = new RelFieldCollation(remappedIndexFieldOrdinal, direction);
            fields.add(fieldCollation);
        }

        return RelCollations.of(fields);
    }

    /**
     * Builds a list of all ascending or all descending field directions for the index fields
     *
     * @param allAscending whether all fields are ascending
     * @return the list of field directions
     */
    private static List<Boolean> buildFieldDirections(MapTableIndex index, boolean allAscending) {
        List<Boolean> ascs = new ArrayList<>(index.getFieldOrdinals().size());

        for (int i = 0; i < index.getFieldOrdinals().size(); ++i) {
            Boolean asc = allAscending ? TRUE : FALSE;
            ascs.add(asc);
        }
        return ascs;
    }

    /**
     * Creates an index scan without any filter.
     *
     * @param scan              the original scan operator
     * @param index             available indexes
     * @param ascs              the collation of index fields
     * @param nonEmptyCollation whether to filter out full index scan with no collation
     * @return index scan or {@code null}
     */
    private static RelNode createFullIndexScan(
            FullScanLogicalRel scan,
            MapTableIndex index,
            List<Boolean> ascs,
            boolean nonEmptyCollation
    ) {
        assert isIndexSupported(index);

        RexNode scanFilter = OptUtils.extractHazelcastTable(scan).getFilter();

        RelTraitSet traitSet = OptUtils.toPhysicalConvention(scan.getTraitSet());

        RelCollation relCollation = buildCollationTrait(scan, index, ascs);
        if (nonEmptyCollation && relCollation.getFieldCollations().size() == 0) {
            // Don't make a full scan with empty collation
            return null;
        }
        traitSet = OptUtils.traitPlus(traitSet, relCollation);

        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable originalHazelcastTable = OptUtils.extractHazelcastTable(scan);

        RelOptTable newRelTable = createRelTable(
                originalRelTable.getDelegate().getQualifiedName(),
                originalHazelcastTable.withFilter(null),
                scan.getCluster().getTypeFactory()
        );

        return new IndexScanMapPhysicalRel(
                scan.getCluster(),
                traitSet,
                newRelTable,
                index,
                null,
                null,
                scanFilter
        );
    }

    /**
     * Composes the final filter from the list of single-column filters.
     *
     * @param filters              single-column filters
     * @param indexType            type of the index
     * @param indexComponentsCount number of components in the index
     * @return final filter or {@code null} if the filter could not be built for the given index type
     */
    private static IndexFilter composeFilter(List<IndexFilter> filters, IndexType indexType, int indexComponentsCount) {
        assert !filters.isEmpty();

        if (indexComponentsCount == 1) {
            // Non-composite index. Just pick the first filter
            assert filters.size() == 1;

            IndexFilter res = filters.get(0);

            assert !(res instanceof IndexRangeFilter) || indexType == SORTED;

            return res;
        } else {
            // At this point component filters has the form "1=EQUALS, 2=EQUALS, ..., N=EQUALS/RANGE/IN".
            // Compose the final filter based on the type of the last resolved filter.
            IndexFilter lastFilter = filters.get(filters.size() - 1);

            if (lastFilter instanceof IndexEqualsFilter) {
                return composeEqualsFilter(filters, (IndexEqualsFilter) lastFilter, indexType, indexComponentsCount);
            } else if (lastFilter instanceof IndexCompositeFilter) {
                return composeCompositeFilter(filters, (IndexCompositeFilter) lastFilter, indexType, indexComponentsCount);
            } else {
                assert lastFilter instanceof IndexRangeFilter;

                assert indexType == SORTED;

                return composeRangeFilter(filters, (IndexRangeFilter) lastFilter, indexType, indexComponentsCount);
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
            if (indexType == HASH) {
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
     * Create the final composite filter from the collection of per-column filters.
     * <p>
     * Consider the expression {@code {a=1 AND b IN (2,3)}}. After the conversion, the composite filter will be
     * {@code {a,b} IN {{1, 2}, {1, 3}}}.
     *
     * @param filters              per-column filters
     * @param lastFilter           the last composite filter
     * @param indexComponentsCount the number of index components
     * @return composite filter
     */
    private static IndexFilter composeCompositeFilter(
            List<IndexFilter> filters,
            IndexCompositeFilter lastFilter,
            IndexType indexType,
            int indexComponentsCount
    ) {
        List<IndexFilter> newFilters = new ArrayList<>(lastFilter.getFilters().size());

        for (IndexFilter filter : lastFilter.getFilters()) {
            if (filter instanceof IndexEqualsFilter) {
                IndexFilter newFilter = composeEqualsFilter(filters, (IndexEqualsFilter) filter, indexType, indexComponentsCount);

                if (newFilter == null) {
                    // Cannot create a filter for one of the values of the IN clause. Stop.
                    return null;
                }

                newFilters.add(newFilter);
            } else if (filter instanceof IndexRangeFilter) {
                IndexFilter newFilter = composeRangeFilter(filters, (IndexRangeFilter) filter, indexType, indexComponentsCount);
                newFilters.add(newFilter);
            }
        }

        return new IndexCompositeFilter(newFilters);
    }

    /**
     * Create the composite range filter from the given per-column filters.
     * <p>
     * If the number of per-column filters if less than the number of index components, then infinite ranges are added
     * to the missing components.
     * <p>
     * Consider that we have two per-column filter as input: {@code {a=1}, {b>2 AND b<3}}.
     * <p>
     * If the index is defined as {@code {a, b}}, then the resulting filter would be {@code {a=1, b>2 AND a=1, b<3}}.
     * <p>
     * If the index is defined as {@code {a, b, c}}, then the resulting filter would be
     * {@code {a=1, b>2, c>NEGATIVE_INFINITY AND a=1, b<3, c<POSITIVE_INFINITY}}.
     *
     * @param filters         all per-column filters
     * @param lastFilter      the last filter (range)
     * @param indexType
     * @param componentsCount number of components in the filter
     * @return range filter
     */
    private static IndexFilter composeRangeFilter(
            List<IndexFilter> filters,
            IndexRangeFilter lastFilter,
            IndexType indexType,
            int componentsCount
    ) {
        if (indexType == HASH) {
            return null;
        }

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
     * @param filters    column filters
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
     * @param fromComponents  expressions for the "from" part
     * @param toComponents    expressions for the "to" part
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
        return index.getType() == SORTED || index.getType() == HASH;
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

                QueryDataTypeFamily fromFamily = HazelcastTypeUtils.toHazelcastType(fromType).getTypeFamily();
                QueryDataTypeFamily toFamily = HazelcastTypeUtils.toHazelcastType(toType).getTypeFamily();

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
}
