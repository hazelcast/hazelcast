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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.metadataQuery;
import static com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRule.Config.DEFAULT;

@Value.Enclosing
public final class StreamToStreamJoinPhysicalRule extends RelRule<RelRule.Config> {
    private Tuple3<Integer, Integer, Long> leftBound;
    private Tuple3<Integer, Integer, Long> rightBound;

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

    @SuppressWarnings("ConstantConditions")
    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        JoinRelType joinType = logicalJoin.getJoinType();
        if (!(joinType == JoinRelType.INNER || joinType.isOuterJoin())) {
            call.transformTo(
                    fail(logicalJoin, "Stream to stream JOIN supports INNER and LEFT/RIGHT OUTER JOIN types.")
            );
        }

        // TODO: detect postpone map from previous join rels
        RelNode left = call.rel(1);
        RelNode right = call.rel(2);

        // TODO WE NEED WATERMARK KEYS TO BE PRESENT HERE! MUST HAVE!
        WatermarkedFields leftWms = metadataQuery(left).extractWatermarkedFields(left);
        WatermarkedFields rightWms = metadataQuery(right).extractWatermarkedFields(right);
        if (leftWms == null || leftWms.isEmpty()) {
            call.transformTo(
                    fail(logicalJoin, "Left input of stream to stream JOIN doesn't contain watermarked columns")
            );
        }
        if (rightWms == null || rightWms.isEmpty()) {
            call.transformTo(
                    fail(logicalJoin, "Right input of stream to stream JOIN doesn't contain watermarked columns")
            );
        }

        RexNode predicate = logicalJoin.analyzeCondition().getRemaining(logicalJoin.getCluster().getRexBuilder());
        if (!(predicate instanceof RexCall)) {
            call.transformTo(
                    fail(logicalJoin, "Stream to stream JOIN condition should contain time boundness predicate")
            );
        }

        BoundsExtractorVisitor visitor = new BoundsExtractorVisitor(logicalJoin);
        predicate.accept(visitor);

        if (!visitor.isValid) {
            call.transformTo(
                    fail(logicalJoin, "Stream to stream JOIN condition should contain time boundness predicate")
            );
        }

        leftBound = visitor.leftBound;
        rightBound = visitor.rightBound;

        call.transformTo(
                new StreamToStreamJoinPhysicalRel(
                        logicalJoin.getCluster(),
                        logicalJoin.getTraitSet().replace(PHYSICAL),
                        RelRule.convert(left, left.getTraitSet().replace(PHYSICAL)),
                        RelRule.convert(right, right.getTraitSet().replace(PHYSICAL)),
                        logicalJoin.getCondition(),
                        logicalJoin.getJoinType(),
                        leftBound,
                        rightBound
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
        private final WatermarkedFields leftWms;
        private final WatermarkedFields rightWms;

        public Tuple3<Integer, Integer, Long> leftBound = null;
        public Tuple3<Integer, Integer, Long> rightBound = null;
        public boolean isValid = true;

        private BoundsExtractorVisitor(Join joinRel) {
            super(true);
            this.joinRel = joinRel;

            this.leftWms = metadataQuery(joinRel.getLeft()).extractWatermarkedFields(joinRel.getLeft());
            this.rightWms = metadataQuery(joinRel.getRight()).extractWatermarkedFields(joinRel.getRight());
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
                        leftBound = tuple3(leftOp.getIndex(), rightOp.getIndex(), 0L);
                        rightBound = tuple3(leftOp.getIndex(), rightOp.getIndex(), 0L);
                    }
                    return null;
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    // We assume that right operand is right bound
                    rightBound = extractBoundFromComparisonCall(call, rightWms);
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    // We assume that right operand is left bound
                    leftBound = extractBoundFromComparisonCall(call, leftWms);
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
        private Tuple3<Integer, Integer, Long> extractBoundFromComparisonCall(RexCall call, WatermarkedFields fields) {
            assert call.getOperands().get(0) instanceof RexInputRef;
            RexInputRef leftOp = (RexInputRef) call.getOperands().get(0);

            RexNode rightOp = call.getOperands().get(1);
            // TODO: if `rightOp` is a literal, -- it's not a boundness condition,
            //  but may be applicable as additional condition.
            if (rightOp instanceof RexCall) {
                Tuple2<Integer, Long> timeBound = extractTimeBoundFromAddition((RexCall) rightOp, fields);
                return tuple3(leftOp.getIndex(), timeBound.f0(), timeBound.f1());
            } else if (rightOp instanceof RexInputRef) {
                return tuple3(leftOp.getIndex(), ((RexInputRef) rightOp).getIndex(), 0L);
            } else {
                isValid = false;
                return null;
            }
        }

        /**
         * Extract time boundness from PLUS/MINUS calls represented as `$ref + constant`.
         */
        private Tuple2<Integer, Long> extractTimeBoundFromAddition(RexCall call, WatermarkedFields fields) {
            // Only plus/minus expressions are supported.
            // Also, one of the operands should be a constant.
            if (!(call.isA(SqlKind.PLUS) || call.isA(SqlKind.MINUS))) {
                isValid = false;
                return tuple2(-1, 0L);
            }

            // TODO: current state is assuming `$ref + constant interval literal`
            Tuple2<RexNode, RexNode> operands = tuple2(call.getOperands().get(0), call.getOperands().get(1));

            RexInputRef inputRef = null;
            RexLiteral literal = null;
            if (operands.f0() instanceof RexInputRef) {
                inputRef = (RexInputRef) operands.f0();
            } else if (operands.f0() instanceof RexLiteral) {
                literal = (RexLiteral) operands.f0();
            }

            if (operands.f1() instanceof RexLiteral) {
                literal = (RexLiteral) operands.f1();
            } else if (operands.f1() instanceof RexInputRef) {
                if (inputRef == null) {
                    inputRef = (RexInputRef) operands.f1();
                } else {
                    // we don't support non-constants within bounds equations.
                    isValid = false;
                    return tuple2(-1, 0L);
                }
            }

            if (inputRef == null || literal == null || !SqlTypeName.INTERVAL_TYPES.contains(literal.getTypeName())) {
                isValid = false;
                return tuple2(-1, 0L);
            }
            return tuple2(inputRef.getIndex(), literal.getValueAs(Long.class));
        }
    }
}
