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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.Map;

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
        JoinLogicalRel logicalJoin = call.rel(0);

        JoinRelType joinType = logicalJoin.getJoinType();
        if (!(joinType == JoinRelType.INNER || joinType.isOuterJoin())) {
            call.transformTo(
                    fail(logicalJoin, "Stream to stream JOIN supports INNER and LEFT/RIGHT OUTER JOIN types.")
            );
        }

        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        WatermarkedFields leftWms = metadataQuery(left).extractWatermarkedFields(left);
        WatermarkedFields rightWms = metadataQuery(right).extractWatermarkedFields(right);
        // region checks
        if (leftWms == null || leftWms.isEmpty()) {
            call.transformTo(
                    fail(logicalJoin, "Left input of stream to stream JOIN doesn't contain watermarked columns")
            );
            return;
        }
        if (rightWms == null || rightWms.isEmpty()) {
            call.transformTo(
                    fail(logicalJoin, "Right input of stream to stream JOIN doesn't contain watermarked columns")
            );
            return;
        }

        RexNode predicate = logicalJoin.analyzeCondition().getRemaining(logicalJoin.getCluster().getRexBuilder());
        if (!(predicate instanceof RexCall)) {
            call.transformTo(
                    fail(logicalJoin, "Stream to stream JOIN condition should contain time boundness predicate")
            );
            return;
        }

        BoundsExtractorVisitor visitor = new BoundsExtractorVisitor(logicalJoin);
        predicate.accept(visitor);

        if (!visitor.isValid) {
            call.transformTo(
                    fail(logicalJoin, "Stream to stream JOIN condition should contain time boundness predicate")
            );
            return;
        }
        // endregion

        Map<OptUtils.RelField, Map<OptUtils.RelField, Long>> postponeMap = new HashMap<>();

        RelNode leftConverted = RelRule.convert(left, left.getTraitSet().replace(PHYSICAL));
        if (leftConverted instanceof RelSubset) {
            if (((RelSubset) leftConverted).getBest() instanceof StreamToStreamJoinPhysicalRel) {
                RelSubset relSubset = (RelSubset) leftConverted;
                StreamToStreamJoinPhysicalRel streamToStreamJoin = (StreamToStreamJoinPhysicalRel) relSubset.getBest();
                postponeMap.putAll(streamToStreamJoin.postponeTimeMap());
            }
        }

        OptUtils.RelField llRelation = new OptUtils.RelField(
                visitor.leftBound.f0().getName(),
                visitor.leftBound.f0().getType()
        );
        OptUtils.RelField lrRelation = new OptUtils.RelField(
                visitor.leftBound.f1().getName(),
                visitor.leftBound.f1().getType()
        );
        OptUtils.RelField rlRelation = new OptUtils.RelField(
                visitor.rightBound.f0().getName(),
                visitor.rightBound.f0().getType()
        );
        OptUtils.RelField rrRelation = new OptUtils.RelField(
                visitor.rightBound.f1().getName(),
                visitor.rightBound.f1().getType()
        );

        if (postponeMap.get(llRelation) == null) {
            Map<OptUtils.RelField, Long> internalMap = new HashMap<>();
            internalMap.put(lrRelation, visitor.leftBound.f2());
            postponeMap.put(llRelation, internalMap);
        } else {
            postponeMap.get(llRelation).put(lrRelation, visitor.leftBound.f2());
        }

        if (postponeMap.get(rrRelation) == null) {
            Map<OptUtils.RelField, Long> internalMap = new HashMap<>();
            internalMap.put(rlRelation, visitor.rightBound.f2());
            postponeMap.put(rrRelation, internalMap);
        } else {
            postponeMap.get(rrRelation).put(rlRelation, visitor.rightBound.f2());
        }


        call.transformTo(
                new StreamToStreamJoinPhysicalRel(
                        logicalJoin.getCluster(),
                        logicalJoin.getTraitSet().replace(PHYSICAL),
                        RelRule.convert(left, left.getTraitSet().replace(PHYSICAL)),
                        RelRule.convert(right, right.getTraitSet().replace(PHYSICAL)),
                        logicalJoin.getCondition(),
                        logicalJoin.getJoinType(),
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

    @SuppressWarnings("CheckStyle")
    private static final class BoundsExtractorVisitor extends RexVisitorImpl<Void> {
        private final Join joinRel;
        private final WatermarkedFields leftInputWms;
        private final WatermarkedFields rightInputWms;

        public Tuple3<RelDataTypeField, RelDataTypeField, Long> leftBound = null;
        public Tuple3<RelDataTypeField, RelDataTypeField, Long> rightBound = null;
        public boolean isValid = true;

        private BoundsExtractorVisitor(Join joinRel) {
            super(true);
            this.joinRel = joinRel;
            this.leftInputWms = metadataQuery(joinRel.getLeft()).extractWatermarkedFields(joinRel.getLeft());
            this.rightInputWms = metadataQuery(joinRel.getRight()).extractWatermarkedFields(joinRel.getRight());
        }

        // `a BETWEEN b and c` ---> `a >= b and a <= c`
        @Override
        public Void visitCall(RexCall call) {
            switch (call.getKind()) {
                case AND:
                    return super.visitCall(call);
                case EQUALS:
                    // TODO: enhance it.
                    if (call.getOperands().get(0) instanceof RexInputRef &&
                            call.getOperands().get(1) instanceof RexInputRef) {
                        RexInputRef leftOp = (RexInputRef) call.getOperands().get(0);
                        RexInputRef rightOp = (RexInputRef) call.getOperands().get(1);
                        RelDataTypeField leftField = joinRel.getRowType().getFieldList().get(leftOp.getIndex());
                        RelDataTypeField rightField = joinRel.getRowType().getFieldList().get(rightOp.getIndex());
                        leftBound = tuple3(leftField, rightField, 0L);
                        rightBound = tuple3(leftField, rightField, 0L);
                    }
                    return null;
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    // We assume that right operand is right bound
                    rightBound = extractBoundFromComparisonCall(call, leftInputWms, rightInputWms);
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    // We assume that right operand is left bound
                    leftBound = extractBoundFromComparisonCall(call, leftInputWms, rightInputWms);
                    break;
                default:
                    return null;
            }
            return super.visitCall(call);
        }

        /**
         * Extract time boundness from GTE/LTE calls
         * represented as `$ref1 >= $ref2 + constant1`
         * or `$ref1 <= $ref2 + constant2`.
         *
         * @return Tuple [ref1_index, ref2_index, bound]
         */
        private Tuple3<RelDataTypeField, RelDataTypeField, Long> extractBoundFromComparisonCall(
                RexCall call,
                WatermarkedFields leftFields,
                WatermarkedFields rightFields
        ) {
            assert call.getOperands().get(0) instanceof RexInputRef;
            RexInputRef leftOp = (RexInputRef) call.getOperands().get(0);
            RexNode rightOp = call.getOperands().get(1);

            RelDataTypeField leftField = joinRel.getRowType().getFieldList().get(leftOp.getIndex());

            if (rightOp instanceof RexCall) {
                Tuple2<RelDataTypeField, Long> timeBound = extractTimeBoundFromAddition(
                        (RexCall) rightOp,
                        leftFields,
                        rightFields);
                return isValid ? tuple3(leftField, timeBound.f0(), timeBound.f1()) : null;
            } else if (rightOp instanceof RexInputRef) {
                RexInputRef rightIRef = (RexInputRef) rightOp;
                RelDataTypeField rightField = joinRel.getRowType().getFieldList().get((rightIRef).getIndex());
                return tuple3(leftField, rightField, 0L);
            } else {
                isValid = false;
                return null;
            }
        }

        /**
         * Extract time boundness from PLUS/MINUS calls represented as `$ref + constant`.
         */
        private Tuple2<RelDataTypeField, Long> extractTimeBoundFromAddition(
                RexCall call,
                WatermarkedFields leftFields,
                WatermarkedFields rightFields) {
            // Only plus/minus expressions are supported.
            // Also, one of the operands should be a constant.
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
                if (!(leftFields.getPropertiesByIndex().containsValue(inputRef)
                        || rightFields.getPropertiesByIndex().containsValue(inputRef))) {
                    isValid = false;
                    return tuple2(null, 0L);
                }
            } else if (operands.f0() instanceof RexLiteral) {
                // Note: straightforward way : `constant + $ref`
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
            if (inputRef == null /*(|| !fields.getPropertiesByIndex().containsValue(inputRef)*/) {
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

            return tuple2(joinRel.getRowType().getFieldList().get(inputRef.getIndex()), literalValue);
        }
    }
}
