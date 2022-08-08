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

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.metadataQuery;
import static com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRule.Config.DEFAULT;

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
    static final RelOptRule STREAM_TO_STREAM_JOIN_RULE = new StreamToStreamJoinPhysicalRule(DEFAULT);

    private StreamToStreamJoinPhysicalRule(StreamToStreamJoinPhysicalRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel join = call.rel(0);

        JoinRelType joinType = join.getJoinType();
        if (!(joinType == JoinRelType.INNER || joinType.isOuterJoin())) {
            call.transformTo(
                    fail(join, "Stream to stream JOIN supports INNER and LEFT/RIGHT OUTER JOIN types."));
        }

        RexBuilder rb = join.getCluster().getRexBuilder();

        RelNode left = RelRule.convert(join.getLeft(), join.getTraitSet().replace(PHYSICAL));
        RelNode right = RelRule.convert(join.getRight(), join.getTraitSet().replace(PHYSICAL));

        WatermarkedFields leftFields = metadataQuery(left).extractWatermarkedFields(left);
        WatermarkedFields rightFields = metadataQuery(right).extractWatermarkedFields(right);

        // region checks
        if (leftFields == null || leftFields.isEmpty()) {
            call.transformTo(fail(join, "Left input of stream-to-stream JOIN must contain watermarked columns"));
            return;
        }

        if (!watermarkedFieldsAreTemporalTypeCheck(join.getLeft(), leftFields)) {
            call.transformTo(fail(join, "Left input of stream-to-stream JOIN watermarked columns are not temporal"));
            return;
        }

        if (rightFields == null || rightFields.isEmpty()) {
            call.transformTo(fail(join, "Right input of stream-to-stream JOIN must contain watermarked columns"));
            return;
        }

        if (!watermarkedFieldsAreTemporalTypeCheck(join.getRight(), rightFields)) {
            call.transformTo(fail(join, "Right input of stream-to-stream JOIN watermarked columns are not temporal"));
            return;
        }

        WatermarkedFields watermarkedFields = watermarkedFields(join, leftFields, rightFields);
        if (watermarkedFields.isEmpty()) {
            call.transformTo(fail(join, "Stream-to-stream JOIN must contain watermarked columns"));
            return;
        }

        BoundsExtractorVisitor visitor = new BoundsExtractorVisitor(watermarkedFields);
        RexNode predicate;
        if (join.analyzeCondition().isEqui()) {
            predicate = join.getCondition();
        } else {
            predicate = join.analyzeCondition().getRemaining(rb);
        }

        if (!(predicate instanceof RexCall)) {
            call.transformTo(fail(join, visitor.errorMessage));
            return;
        }

        predicate.accept(visitor);

        if (!visitor.isValid) {
            call.transformTo(fail(join, visitor.errorMessage));
            return;
        }
        // endregion

        Map<Integer, Map<Integer, Long>> postponeMap = assemblePostponeTimeMap(visitor);

        Map<Integer, Integer> leftInputToJointRowMapping = new HashMap<>();
        Map<Integer, Integer> rightInputToJointRowMapping = new HashMap<>();

        // calculate field refs mapping from joint row to input rows to
        int i;
        for (i = 0; i < left.getRowType().getFieldList().size(); ++i) {
            leftInputToJointRowMapping.put(i, i);
        }
        for (int j = 0; j < right.getRowType().getFieldList().size(); ++j) {
            rightInputToJointRowMapping.put(i++, j);
        }

        call.transformTo(
                new StreamToStreamJoinPhysicalRel(
                        join.getCluster(),
                        join.getTraitSet().replace(PHYSICAL),
                        RelRule.convert(call.rel(1), call.rel(1).getTraitSet().replace(PHYSICAL)),
                        RelRule.convert(call.rel(2), call.rel(2).getTraitSet().replace(PHYSICAL)),
                        join.getCondition(),
                        join.getJoinType(),
                        leftFields,
                        rightFields,
                        leftInputToJointRowMapping,
                        rightInputToJointRowMapping,
                        postponeMap
                )
        );
    }

    private MustNotExecutePhysicalRel fail(RelNode node, String message) {
        return new MustNotExecutePhysicalRel(
                node.getCluster(),
                node.getTraitSet().replace(PHYSICAL),
                node.getRowType(),
                message
        );
    }

    private Map<Integer, Map<Integer, Long>> assemblePostponeTimeMap(BoundsExtractorVisitor visitor) {
        Map<Integer, Map<Integer, Long>> postponeMap = new HashMap<>();

        Map<Integer, Long> leftBoundMap = new HashMap<>();
        leftBoundMap.put(visitor.leftBound.f1(), visitor.leftBound.f2());
        postponeMap.put(visitor.leftBound.f0(), leftBoundMap);

        Map<Integer, Long> rightBoundMap = new HashMap<>();
        rightBoundMap.put(visitor.rightBound.f1(), visitor.rightBound.f2());
        postponeMap.put(visitor.rightBound.f0(), rightBoundMap);
        return postponeMap;
    }

    /**
     * Extracts all watermarked fields represented in JOIN relation row type.
     *
     * @return left, right input and joint watermarked fields from rel tree
     */
    private WatermarkedFields watermarkedFields(
            JoinLogicalRel join,
            WatermarkedFields leftFields,
            WatermarkedFields rightFields) {
        final int offset = join.getLeft().getRowType().getFieldList().size();
        Set<Integer> shiftedRightProps = rightFields.getFieldIndexes()
                .stream()
                .map(right -> right + offset)
                .collect(Collectors.toSet());

        return leftFields.union(new WatermarkedFields(shiftedRightProps));
    }

    /**
     * Checks if all watermarked fields are temporal type.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean watermarkedFieldsAreTemporalTypeCheck(RelNode input, WatermarkedFields fields) {
        for (Integer idx : fields.getFieldIndexes()) {
            RelDataType type = input.getRowType().getFieldList().get(idx).getType();
            if (!HazelcastTypeUtils.isTemporalType(type)) {
                return false;
            }
        }
        return true;
    }


    @SuppressWarnings("CheckStyle")
    private static final class BoundsExtractorVisitor extends RexVisitorImpl<Void> {
        private final WatermarkedFields watermarkedFields;
        private final String defaultErrorMessage = "Stream-to-stream JOIN condition must contain time boundness predicate";
        private String errorMessage = null;

        public Tuple3<Integer, Integer, Long> leftBound = null;
        public Tuple3<Integer, Integer, Long> rightBound = null;

        public boolean isValid = true;

        private BoundsExtractorVisitor(WatermarkedFields watermarkedFields) {
            super(true);
            this.watermarkedFields = watermarkedFields;
        }

        public String errorMessage() {
            return errorMessage == null ? defaultErrorMessage : errorMessage;
        }

        // Note: we're expecting equality or `a BETWEEN b and c` call here. Anything else will be rejected.
        @Override
        public Void visitCall(RexCall call) {
            switch (call.getKind()) {
                case AND:
                    return super.visitCall(call);
                case EQUALS:
                    List<RexNode> operands = call.getOperands();
                    if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef) {
                        RexInputRef leftOp = (RexInputRef) operands.get(0);
                        RexInputRef rightOp = (RexInputRef) operands.get(1);
                        leftBound = tuple3(leftOp.getIndex(), rightOp.getIndex(), 0L);
                        rightBound = tuple3(rightOp.getIndex(), leftOp.getIndex(), 0L);
                    } else {
                        isValid = false;
                        errorMessage = "Time boundness or time equality condition are supported for stream-to-stream JOIN";
                    }
                    return null;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    // We assume that right operand is left bound
                    leftBound = extractBoundFromComparisonCall(call, watermarkedFields, true);
                    break;
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    // We assume that right operand is right bound
                    rightBound = extractBoundFromComparisonCall(call, watermarkedFields, false);
                    break;
                default:
                    return null;
            }
            return super.visitCall(call);
        }

        /**
         * Extract time boundness from GTE/LTE calls represented as
         * `$ref1 >= $ref2 + constant1` or `$ref1 <= $ref2 + constant2`.
         * <p>
         * It's needed to enlighten mathematical representation for group of canonical time-bound in-equations.
         * Long story short, we reduce existing bounds to such in-equations system:
         * <pre>
         * right.time >= left.time - left_bound
         * left.time >= right.time - right_bound
         * </pre>
         * Then, we represent this in-equations system as so-called 'postpone map':
         * <pre>
         * [left_wm_key -> [right_wm_key -> left_bound]]
         * [right_wm_key -> [left_wm_key -> right_bound]]
         * </pre>
         * <p>
         * <a href=https://github.com/Fly-Style/hazelcast/blob/feat/5.2/stream-to-stream-join-design/docs/design/sql/15-stream-to-stream-join.md#implementation-of-multiple-watermarks-in-the-join-processor>Read more.</a>
         *
         * @return Tuple [ref1_index, ref2_index, bound]
         */
        private Tuple3<Integer, Integer, Long> extractBoundFromComparisonCall(
                RexCall call,
                WatermarkedFields wmFields,
                boolean isLeft
        ) {
            assert call.getOperands().get(0) instanceof RexInputRef;
            RexInputRef leftOp = (RexInputRef) call.getOperands().get(0);
            RexNode rightOp = call.getOperands().get(1);

            Tuple3<Integer, Integer, Long> boundToReturn = null;

            if (rightOp instanceof RexCall) {
                Tuple2<Integer, Long> timeBound = extractTimeBoundFromBinaryOp((RexCall) rightOp, wmFields);
                boundToReturn = isValid ? tuple3(leftOp.getIndex(), timeBound.f0(), timeBound.f1()) : null;
            } else if (rightOp instanceof RexInputRef) {
                RexInputRef rightIRef = (RexInputRef) rightOp;
                boundToReturn = tuple3(leftOp.getIndex(), rightIRef.getIndex(), 0L);
            } else {
                isValid = false;
            }
            if (isValid && isLeft) {
                boundToReturn = tuple3(boundToReturn.f1(), boundToReturn.f0(), -boundToReturn.f2());
            }

            return boundToReturn;
        }

        /**
         * Extract time boundness from PLUS/MINUS calls represented as `$ref + constant`.
         */
        private Tuple2<Integer, Long> extractTimeBoundFromBinaryOp(
                RexCall call,
                WatermarkedFields watermarkedFields) {
            if (!(call.isA(SqlKind.PLUS) || call.isA(SqlKind.MINUS))) {
                isValid = false;
                return tuple2(null, 0L);
            }

            // Assuming only `$ref + constant interval literal` support.
            Tuple2<RexNode, RexNode> operands = tuple2(call.getOperands().get(0), call.getOperands().get(1));

            RexInputRef inputRef = null;
            RexLiteral literal = null;
            // Note: straightforward way : `$ref + constant interval literal`
            if (operands.f0() instanceof RexInputRef) {
                inputRef = (RexInputRef) operands.f0();
                if (!watermarkedFields.getFieldIndexes().contains(inputRef.getIndex())) {
                    isValid = false;
                    return tuple2(null, 0L);
                }
            } else if (operands.f0() instanceof RexLiteral) {
                // Note: opposite way : `constant + $ref`
                literal = (RexLiteral) operands.f0();
            }

            if (operands.f1() instanceof RexLiteral) {
                literal = (RexLiteral) operands.f1();
            } else if (operands.f1() instanceof RexInputRef) {
                if (inputRef == null) {
                    inputRef = (RexInputRef) operands.f1();
                } else {
                    // It means situation $ref_1 + $ref_2
                    // we don't support non-constants within bounds equations.
                    isValid = false;
                    return tuple2(null, 0L);
                }
            }

            // if input ref is not watermarked -- invalid.
            if (inputRef == null) {
                isValid = false;
                return tuple2(null, 0L);
            }

            if (literal == null || !SqlTypeName.INTERVAL_TYPES.contains(literal.getTypeName())) {
                isValid = false;
                return tuple2(null, 0L);
            }

            Long literalValue = literal.getValueAs(Long.class);
            if (call.isA(SqlKind.MINUS)) {
                literalValue *= -1;
            }

            return tuple2(inputRef.getIndex(), literalValue);
        }
    }
}
