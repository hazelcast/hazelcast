/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.metadataQuery;
import static com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRule.Config.DEFAULT;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.hasSameTypeFamily;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isNumericIntegerType;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.isTemporalType;

@Value.Enclosing
public final class StreamToStreamJoinPhysicalRule extends RelRule<RelRule.Config> {
    @Value.Immutable
    public interface Config extends RelRule.Config {
        StreamToStreamJoinPhysicalRule.Config DEFAULT = ImmutableStreamToStreamJoinPhysicalRule.Config.builder()
                .description(StreamToStreamJoinPhysicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(JoinLogicalRel.class)
                        .trait(LOGICAL)
                        .inputs(
                                b1 -> b1.operand(RelNode.class)
                                        .predicate(OptUtils::isUnbounded)
                                        .anyInputs(),
                                b2 -> b2.operand(RelNode.class)
                                        .predicate(OptUtils::isUnbounded)
                                        .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new StreamToStreamJoinPhysicalRule(this);
        }
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new StreamToStreamJoinPhysicalRule(DEFAULT);

    private StreamToStreamJoinPhysicalRule(StreamToStreamJoinPhysicalRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel join = call.rel(0);
        RelNode leftInput = call.rel(1);
        RelNode rightInput = call.rel(2);

        JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT && joinType != JoinRelType.RIGHT) {
            call.transformTo(
                    fail(join, "Stream to stream JOIN supports INNER and LEFT/RIGHT OUTER JOIN types"));
        }

        RelNode left = RelRule.convert(leftInput, leftInput.getTraitSet().replace(PHYSICAL));
        RelNode right = RelRule.convert(rightInput, rightInput.getTraitSet().replace(PHYSICAL));

        WatermarkedFields leftFields = metadataQuery(left).extractWatermarkedFields(left);
        WatermarkedFields rightFields = metadataQuery(right).extractWatermarkedFields(right);

        if (leftFields == null || rightFields == null) {
            // If we pass initial isUnbounded checks for left & right input with rule config,
            // it means, we cannot execute such a query, we must abort it.
            String message = "For stream-to-stream join, both joined sides must have an order imposed";
            call.transformTo(fail(join, message));
            return;
        }

        WatermarkedFields wmFields = watermarkedFields(join, leftFields, rightFields);

        // a postponeTimeMap just like the one described in the TDD, but we don't use WM keys, but field indexes here
        Map<Integer, Map<Integer, Long>> postponeTimeMap = new HashMap<>();

        // extract time bounds from the join condition
        for (RexNode conjunction : RelOptUtil.conjunctions(join.getCondition())) {
            tryExtractTimeBound(conjunction, wmFields.getFieldIndexes(), postponeTimeMap);
        }

        // check that there is at least one bound for the left time, involving right time, and one for
        // right time, involving left time
        int leftColumns = join.getLeft().getRowType().getFieldCount();
        boolean foundLeft = false;
        boolean foundRight = false;
        for (Entry<Integer, Map<Integer, Long>> enOuter : postponeTimeMap.entrySet()) {
            for (Iterator<Entry<Integer, Long>> innerIt = enOuter.getValue().entrySet().iterator(); innerIt.hasNext(); ) {
                Entry<Integer, Long> enInner = innerIt.next();
                if (enOuter.getKey() < leftColumns) {
                    if (enInner.getKey() < leftColumns) {
                        innerIt.remove();
                        continue;
                    }
                    foundLeft = true;
                } else {
                    if (enInner.getKey() >= leftColumns) {
                        innerIt.remove();
                        continue;
                    }
                    foundRight = true;
                }
            }
        }

        if (!foundLeft || !foundRight) {
            List<String> leftWatermarkedColumnNames = leftFields.getFieldIndexes().stream()
                    .map(left.getRowType().getFieldNames()::get)
                    .collect(Collectors.toList());
            List<String> rightWatermarkedColumnNames = rightFields.getFieldIndexes().stream()
                    .map(right.getRowType().getFieldNames()::get)
                    .collect(Collectors.toList());

            call.transformTo(
                    fail(join, String.format(
                            "A stream-to-stream join must have a join condition constraining the maximum " +
                                    "difference between time values of the joined tables in both directions. " +
                                    "Time columns on the left side: %s, time columns on the right side: %s",
                            // note that the join can be transposed and sides may not match original query
                            leftWatermarkedColumnNames, rightWatermarkedColumnNames)));
            return;
        }

        call.transformTo(
                new StreamToStreamJoinPhysicalRel(
                        join.getCluster(),
                        join.getTraitSet().replace(PHYSICAL),
                        left,
                        right,
                        join.getCondition(),
                        join.getJoinType(),
                        postponeTimeMap
                )
        );
    }

    /**
     * Add one time bound from the condition to the `postponeTimeMap`, if the
     * condition represents a time bound. It checks if the referenced fields are
     * watermarked, but doesn't check, if they are from a different side of the
     * join.
     */
    // package-visible for test
    static void tryExtractTimeBound(
            RexNode condition,
            Set<Integer> wmFieldIndexes,
            Map<Integer, Map<Integer, Long>> postponeTimeMap
    ) {
        /*
        The canonical form is:
            timeA >= timeB - constant

        We allow any form of the expression with addends moved in any order, such as:
            timeA - timeB + constant >= 0
            timeA + constant >= timeB
            timeB <= timeA + constant

        All of the above expressions are equivalent.

        We also allow multiple constants:
            timeA + constant1 >= timeB + constant2

        In the algorithm below we move all the addends to the left side of the comparison
        operator, and we look for one field with positive sign, one with negative sign, and we
        add all the constants. We ignore edge cases, such as `-timeA + timeB`, which is not
        defined, because we don't have `+` operator for instants, nor a unary `-`. But we
        can putatively reorder them to have `timeB - timeA`. We also sum all the constants.
        We ignore cases when we hit an overflow: SQL doesn't prescribe the execution order
        of expressions in cases when `a + b - c` would overflow, but `a - c + b` won't, we are
        still allowed to do such transformation (at least I surmise this to be the case ;-).
        Anyway, when using simple expressions with one field with positive sign on either
        side, and one constant, these edge cases do not happen.
         */

        boolean isGt;
        boolean isLt;

        switch (condition.getKind()) {
            case EQUALS:
                isGt = true;
                isLt = true;
                break;

            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                isGt = true;
                isLt = false;
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                isGt = false;
                isLt = true;
                break;

            case IS_NOT_DISTINCT_FROM:
                // We don't support IS NOT DISTINCT FROM, because in that case we should join rows
                // where the timestamp is null on both sides, and that doesn't allow us to use the time bound,
                // we would have to buffer those rows forever.
                return;

            case BETWEEN:
                // BETWEEN should have been converted to `a >= b AND a <= c` at this point, but if it isn't,
                // rather report it
                throw new RuntimeException("Unexpected BETWEEN");

            default:
                return;
        }

        Integer[] positiveField = {null};
        Integer[] negativeField = {null};
        long[] constantsSum = {0};

        if (!addAddends(((RexCall) condition).getOperands().get(0), positiveField, negativeField, constantsSum, false)
                || !addAddends(((RexCall) condition).getOperands().get(1), positiveField, negativeField, constantsSum, true)) {
            return;
        }

        if (positiveField[0] == null || negativeField[0] == null) {
            // a field is not on both sides
            return;
        }

        if (!wmFieldIndexes.contains(positiveField[0]) || !wmFieldIndexes.contains(negativeField[0])) {
            // some used field isn't watermarked
            return;
        }

        if (isLt) {
            postponeTimeMap
                    .computeIfAbsent(negativeField[0], x -> new HashMap<>())
                    .merge(positiveField[0], constantsSum[0], Long::min);
        }

        if (isGt) {
            postponeTimeMap
                    .computeIfAbsent(positiveField[0], x -> new HashMap<>())
                    .merge(negativeField[0], -constantsSum[0], Long::min);
        }
    }

    private static boolean addAddends(
            RexNode expr,
            Integer[] positiveField,
            Integer[] negativeField,
            long[] constantsSum,
            boolean inverse
    ) {
        if (expr instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) expr;
            if (!SqlTypeName.DAY_INTERVAL_TYPES.contains(literal.getType().getSqlTypeName())
                    && !SqlTypeName.INT_TYPES.contains(literal.getType().getSqlTypeName())) {
                return false;
            }

            Long value = literal.getValueAs(Long.class);
            if (value == null) {
                return false;
            }
            constantsSum[0] += (inverse ? 1 : -1) * value;
            return true;
        }

        if (expr instanceof RexInputRef) {
            Integer[] field = inverse ? positiveField : negativeField;
            if (field[0] != null) {
                return false;
            }
            field[0] = ((RexInputRef) expr).getIndex();
            return true;
        }

        if (expr instanceof RexCall) {
            RexCall call = (RexCall) expr;
            if (call.isA(SqlKind.CAST)) {
                RexNode inputRef = call.getOperands().get(0);
                RelDataType type = inputRef.getType();
                // Only integer and temporal types are supported for watermarking for S2S JOIN.
                if (hasSameTypeFamily(type, call.getType()) && (isNumericIntegerType(type) || isTemporalType(type))) {
                    return addAddends(inputRef, positiveField, negativeField, constantsSum, inverse);
                }
            }
        }

        if (expr.getKind() == SqlKind.PLUS || expr.getKind() == SqlKind.MINUS) {
            // if this is a subtraction, inverse the 2nd operand
            boolean secondOperandInverse = expr.getKind() == SqlKind.MINUS ? !inverse : inverse;

            List<RexNode> operands = ((RexCall) expr).getOperands();
            return addAddends(operands.get(0), positiveField, negativeField, constantsSum, inverse)
                    && addAddends(operands.get(1), positiveField, negativeField, constantsSum, secondOperandInverse);
        }

        return false;
    }

    private MustNotExecutePhysicalRel fail(RelNode node, String message) {
        return new MustNotExecutePhysicalRel(
                node.getCluster(),
                node.getTraitSet().replace(PHYSICAL),
                node.getRowType(),
                message
        );
    }

    /**
     * Extracts all watermarked fields represented in JOIN relation row type.
     *
     * @return left, right input and joined watermarked fields from rel tree
     */
    private WatermarkedFields watermarkedFields(
            JoinLogicalRel join,
            WatermarkedFields leftFields,
            WatermarkedFields rightFields
    ) {
        final int offset = join.getLeft().getRowType().getFieldList().size();
        Set<Integer> shiftedRightProps = rightFields.getFieldIndexes()
                .stream()
                .map(right -> right + offset)
                .collect(Collectors.toSet());

        return leftFields.union(new WatermarkedFields(shiftedRightProps));
    }
}
