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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.config.IndexType;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.distribution.DistributionField;
import com.hazelcast.sql.impl.calcite.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableIndex;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterType;
import com.hazelcast.sql.impl.extract.QueryPath;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.PARTITIONED;

/**
 * Convert logical map scan to either replicated or partitioned physical scan.
 */
public final class MapScanPhysicalRule extends AbstractPhysicalRule {
    public static final RelOptRule INSTANCE = new MapScanPhysicalRule();

    private MapScanPhysicalRule() {
        super(
            OptUtils.single(MapScanLogicalRel.class, HazelcastConventions.LOGICAL),
            MapScanPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch0(RelOptRuleCall call) {
        MapScanLogicalRel scan = call.rel(0);

        if (scan.isReplicated()) {
            PhysicalRel newScan = new ReplicatedMapScanPhysicalRel(
                scan.getCluster(),
                OptUtils.toPhysicalConvention(scan.getTraitSet(), DistributionTrait.REPLICATED_DIST),
                scan.getTable(),
                scan.getProjects(),
                scan.getFilter()
            );

            call.transformTo(newScan);
        } else {
            HazelcastTable table = scan.getTableUnwrapped();

            DistributionTrait distribution = getDistributionTrait(table, scan.getProjects());

            List<RelNode> transforms = new ArrayList<>(1);

            // Add normal map scan.
            transforms.add(new MapScanPhysicalRel(
                scan.getCluster(),
                OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution),
                scan.getTable(),
                scan.getProjects(),
                scan.getFilter()
            ));

            // Try adding index scans.
            addIndexScans(
                scan,
                distribution,
                table.getIndexes(),
                transforms
            );

            for (RelNode transform : transforms) {
                call.transformTo(transform);
            }
        }
    }

    private void addIndexScans(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        List<HazelcastTableIndex> indexes,
        List<RelNode> transforms
    ) {
        RexNode filter = scan.getFilter();

        if (filter == null) {
            return;
        }

        List<RexNode> disjunctions = new ArrayList<>(1);

        RelOptUtil.decomposeDisjunction(filter, disjunctions);

        if (disjunctions.size() > 1) {
            // TODO: Currently we do not support disjunctions. In order to process disjunction with indexes, we must evaluate
            //  every expression separately, and then *PERFORM DEDUP* on results returned from all disjunctions. This easily
            //  converts innocent non-blocking scan into blocking operation requiring potentially large amount of RAM.
            //  In production-ready implementation we must implement two additional optimizations:
            //  1) Convert OR to UNION - this is widely known database technique which will allow us to employ indexes
            //  on isolated subexpressions at the cost of additional memory usage.
            //  2) Analyze OR predicates, and see if they use the same column, and values forms disjunctive ranges. In this
            //  case we know for sure that they will not return duplicates! E.g. (a < 5 or a > 10) - there will be no
            //  duplicates. Or (a IN (1, 2, 3)). But in principle we go even further and try to rewrite overlapping ranges
            //  to non-overlapping ranges. But this will not work for parameters.
            return;
        }

        for (HazelcastTableIndex index : indexes) {
            RelNode transform = tryCreateIndexScan(scan, distribution, index, disjunctions.get(0));

            if (transform != null) {
                transforms.add(transform);
            }
        }
    }

    private RelNode tryCreateIndexScan(
        MapScanLogicalRel scan,
        DistributionTrait distribution,
        HazelcastTableIndex index,
        RexNode exp
    ) {
        // Map index fields to scan fields. Since not all index might be involved in
        List<String> indexAttributes = index.getAttributes();

        if (indexAttributes.size() > 1) {
            // TODO: Do not support composite index in the prototype for the sake of simplicity. Should be supported in
            //  production implementation.
            return null;
        }

        List<Integer> indexAttributes0 = new ArrayList<>(indexAttributes.size());

        List<String> scanFieldNames = scan.getTable().getRowType().getFieldNames();

        for (String indexAttribute : indexAttributes) {
            int pos = scanFieldNames.indexOf(indexAttribute);

            if (pos == -1) {
                break;
            }

            indexAttributes0.add(pos);
        }

        // If the very first index attribute is not present in the scan row type, then this field is never requested, and hence
        // index cannot be used.
        if (indexAttributes0.isEmpty()) {
            return null;
        }

        // If at least one of the fields of hash index is never referred, then it cannot be used for sure.
        if (index.getType() == IndexType.HASH && indexAttributes0.size() != indexAttributes.size()) {
            return null;
        }

        BiTuple<IndexFilter, RexNode> filter = tryCreateIndexFilter(
            index.getType(),
            indexAttributes0,
            exp,
            scan.getCluster().getRexBuilder()
        );

        if (filter == null) {
            return null;
        }

        RelCollation collation = createIndexCollation(scan, index);

        // TODO: We must add collation here (see commented line). Somehow it breaks the planner.
        RelTraitSet traitSet = OptUtils.toPhysicalConvention(scan.getTraitSet(), distribution);
        //  RelTraitSet traitSet = RuleUtils.toPhysicalConvention(scan.getTraitSet(), distribution).plus(collation);

        return new MapIndexScanPhysicalRel(
            scan.getCluster(),
            traitSet,
            scan.getTable(),
            scan.getProjects(),
            index,
            filter.element1(),
            filter.element2(),
            scan.getFilter()
        );
    }

    /**
     * Try creating an index filter from the given conjunctive expression.
     *
     * @param indexType Index type.
     * @param indexAttributes Index attribute.
     * @param baseExp Base conjunctive expression.
     * @return Index filter or {@code null} if an expression cannot be used with the given index.
     */
    private BiTuple<IndexFilter, RexNode> tryCreateIndexFilter(
        IndexType indexType,
        List<Integer> indexAttributes,
        RexNode baseExp,
        RexBuilder rexBuilder
    ) {
        // Decompose conjunctive predicates.
        List<RexNode> exps = new ArrayList<>(1);

        RelOptUtil.decomposeConjunction(baseExp, exps);

        // TODO: Remember to handle IN condition here! Perhaps we should return List<IndexFilter> instead.

        // TODO: We do not bother with ranges at the moment (x > 1 AND x < 3). Do that for production implementation

        // TODO: Use indexes not only for plain comparisons, but for monotonic expressions as well (e.g. "x + A > B")!

        // Now we iterate over single expressions in hope to find a match for a prefix of index attributes.
        List<IndexFilter> indexFilters = new ArrayList<>(1);

        for (int i = 0; i < indexAttributes.size(); i++) {
            indexFilters.add(null);
        }

        List<RexNode> remainderExps = new ArrayList<>(Math.min(exps.size() - 1, 1));

        for (RexNode exp : exps) {
            boolean remainder = false;

            BiTuple<Integer, IndexFilter> positionAndCondition =
                getIndexAttributePositionForExpression(exp, indexType, indexAttributes);

            if (positionAndCondition == null) {
                // Expression cannot be used with index.
                remainder = true;
            } else {
                // Expression could be used with index. Add it to the appropriate position.
                int position = positionAndCondition.element1();
                IndexFilter condition = positionAndCondition.element2();

                assert position < indexAttributes.size();

                IndexFilter oldIndexFilter = indexFilters.get(position);

                if (oldIndexFilter != null) {
                    remainder = true;
                } else {
                    indexFilters.set(position, condition);
                }
            }

            if (remainder) {
                remainderExps.add(exp);
            }
        }

        // Get final index expressions.
        IndexFilter finalIndexFilter = composeIndexConditions(indexFilters);

        if (finalIndexFilter == null) {
            return null;
        }

        RexNode finalRemainderExp = RexUtil.composeConjunction(rexBuilder, remainderExps);

        return BiTuple.of(finalIndexFilter, finalRemainderExp);
    }

    private static IndexFilter composeIndexConditions(List<IndexFilter> indexFilters) {
        if (indexFilters.size() > 1) {
            // TODO: Properly compose condition for composite indexes.
            throw new UnsupportedOperationException("Composite indexes are not supported at the moment.");
        }

        if (indexFilters.get(0) == null) {
            // TODO: Refactor this when composite index support is added. We should check for not-null prefix here.
            return null;
        }

        return indexFilters.get(0);
    }

    @SuppressWarnings({"checkstyle:FallThrough", "checkstyle:CyclomaticComplexity"})
    private static BiTuple<Integer, IndexFilter> getIndexAttributePositionForExpression(
        RexNode exp,
        IndexType indexType,
        List<Integer> indexAttributes
    ) {
        SqlKind kind = exp.getKind();

        RexNode operand1;
        RexNode operand2;

        switch (kind) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                if (indexType == IndexType.HASH) {
                    return null;
                }

            case EQUALS:
                operand1 = ((RexCall) exp).getOperands().get(0);
                operand2 = ((RexCall) exp).getOperands().get(1);

                break;

            default:
                return null;
        }

        // At this point operation is supported by the index. Let's look at arguments. We are looking for RexInputRef on the
        // one side, and literal or argument on the other.

        // TODO: Make sure that parameter placeholders are supported

        // TODO: Think how to support the case a=b, when both a and b are part of the index! Seems that we need to return
        // TODO: all possible combinations from that method.

        if (operand1.getKind() == SqlKind.INPUT_REF && operand2.getKind() == SqlKind.LITERAL) {
            int index = indexAttributes.indexOf(((RexInputRef) operand1).getIndex());

            if (index == -1) {
                return null;
            }

            IndexFilterType type = inferConditionType(kind);
            Object value = ((RexLiteral) operand2).getValue();

            return BiTuple.of(index, new IndexFilter(type, value));
        }

        if (operand2.getKind() == SqlKind.INPUT_REF && operand1.getKind() == SqlKind.LITERAL) {
            int index = indexAttributes.indexOf(((RexInputRef) operand2).getIndex());

            if (index == -1) {
                return null;
            }

            IndexFilterType type = inferConditionType(inverseIndexConditionKind(kind));
            Object value = ((RexLiteral) operand1).getValue();

            return BiTuple.of(index, new IndexFilter(type, value));
        }

        return null;
    }

    private static IndexFilterType inferConditionType(SqlKind kind) {
        switch (kind) {
            case GREATER_THAN:
                return IndexFilterType.GREATER_THAN;

            case GREATER_THAN_OR_EQUAL:
                return IndexFilterType.GREATER_THAN_OR_EQUAL;

            case LESS_THAN:
                return IndexFilterType.LESS_THAN;

            case LESS_THAN_OR_EQUAL:
                return IndexFilterType.LESS_THAN_OR_EQUAL;

            case EQUALS:
                return IndexFilterType.EQUALS;

            default:
                throw new UnsupportedOperationException("Unexpected kind: " + kind);
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

            case EQUALS:
                return kind;

            default:
                throw new UnsupportedOperationException("Unexpected kind: " + kind);
        }
    }

    /**
     * Create collation trait for the given scan and index.
     *
     * @param scan Scan.
     * @param index Index.
     * @return Collation trait.
     */
    private static RelCollation createIndexCollation(MapScanLogicalRel scan, HazelcastTableIndex index) {
        if (index.getType() == IndexType.HASH) {
            // Hash index doesn't enforce any collation.
            return RelCollations.EMPTY;
        }

        assert index.getType() == IndexType.SORTED;

        // Map scan field names to relevant outputs.
        Map<String, Integer> fieldToProjectIndex = new HashMap<>();

        List<String> scanFieldNames = scan.getTable().getRowType().getFieldNames();
        List<Integer> scanProjects = scan.getProjects();

        for (int i = 0; i < scanFieldNames.size(); i++) {
            int projectIndex = scanProjects.indexOf(i);

            if (projectIndex == -1) {
                // Scan field is not projected out.
                continue;
            }

            String fieldName = scanFieldNames.get(i);

            fieldToProjectIndex.put(fieldName, projectIndex);
        }

        // Now add prefix of index attributes.
        List<RelFieldCollation> fieldCollations = new ArrayList<>(index.getAttributes().size());

        for (String attribute : index.getAttributes()) {
            Integer projectIndex = fieldToProjectIndex.get(attribute);

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
     * Get distribution trait for the given table.
     *
     * @param hazelcastTable Table.
     * @param projects Projects.
     * @return Distribution trait.
     */
    private static DistributionTrait getDistributionTrait(HazelcastTable hazelcastTable, List<Integer> projects) {
        List<DistributionField> distributionFields = getDistributionFields(hazelcastTable);

        if (distributionFields.isEmpty()) {
            return DistributionTrait.PARTITIONED_UNKNOWN_DIST;
        } else {
            // Remap internal scan distribution fields to projected fields.
            List<DistributionField> res = new ArrayList<>(distributionFields.size());

            for (DistributionField distributionField : distributionFields) {
                int distributionFieldIndex = distributionField.getIndex();

                int projectIndex = projects.indexOf(distributionFieldIndex);

                if (projectIndex == -1) {
                    return DistributionTrait.PARTITIONED_UNKNOWN_DIST;
                }

                res.add(new DistributionField(projectIndex, distributionField.getNestedField()));
            }

            return DistributionTrait.Builder.ofType(PARTITIONED).addFieldGroup(res).build();
        }
    }

    /**
     * Get distribution field of the given table.
     *
     * @param hazelcastTable Table.
     * @return Distribution field wrapped into a list or an empty list if no distribution field could be determined.
     */
    private static List<DistributionField> getDistributionFields(HazelcastTable hazelcastTable) {
        if (hazelcastTable.isReplicated()) {
            return Collections.emptyList();
        }

        String distributionFieldName = hazelcastTable.getDistributionField();

        int index = 0;

        for (RelDataTypeField field : hazelcastTable.getFieldList()) {
            String path = hazelcastTable.getFieldPath(field.getName());

            if (path.equals(QueryPath.KEY)) {
                // If there is no distribution field, use the whole key.
                if (distributionFieldName == null) {
                    return Collections.singletonList(new DistributionField(index));
                }

                // TODO: Enable this for nested field support.
//                // Otherwise try to find desired field as a nested field of the key.
//                for (RelDataTypeField nestedField : field.getType().getFieldList()) {
//                    String nestedFieldName = nestedField.getName();
//
//                    if (nestedField.getName().equals(distributionField)) {
//                        return Collections.singletonList(new DistributionField(index, nestedFieldName));
//                    }
//                }
            } else {
                // Try extracting the field from the key-based path and check if it is the distribution field.
                // E.g. "field" -> (attribute) -> "__key.distField" -> (strategy) -> "distField".
                String keyPath = QueryPath.extractKeyPath(path);

                if (keyPath != null) {
                    if (keyPath.equals(distributionFieldName)) {
                        return Collections.singletonList(new DistributionField(index));
                    }
                }
            }

            index++;
        }

        return Collections.emptyList();
    }
}
