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
import com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinP;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.metadataQuery;
import static com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields.join;
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
                    fail(join, "Stream to stream JOIN supports INNER and LEFT/RIGHT OUTER JOIN types.")
            );
        }

        RelNode left = RelRule.convert(join.getLeft(), join.getTraitSet().replace(PHYSICAL));
        RelNode right = RelRule.convert(join.getRight(), join.getTraitSet().replace(PHYSICAL));

        WatermarkedFields leftFields = metadataQuery(join).extractWatermarkedFields(left);
        WatermarkedFields rightFields = metadataQuery(join).extractWatermarkedFields(right);

        // region checks
        if (leftFields == null || leftFields.isEmpty()) {
            call.transformTo(
                    fail(join, "Left input of stream to stream JOIN must contain watermarked columns")
            );
            return;
        }

        if (rightFields == null || rightFields.isEmpty()) {
            call.transformTo(
                    fail(join, "Right input of stream to stream JOIN must contain watermarked columns")
            );
            return;
        }

        Tuple3<WatermarkedFields, WatermarkedFields, WatermarkedFields> watermarkedFields = watermarkedFields(
                join,
                leftFields,
                rightFields);
        if (watermarkedFields.f2() == null || watermarkedFields.f2().isEmpty()) {
            call.transformTo(
                    fail(join, "Stream to stream JOIN must contain watermarked columns")
            );
            return;
        }

        RexNode predicate = join.analyzeCondition().getRemaining(join.getCluster().getRexBuilder());
        if (!(predicate instanceof RexCall)) {
            call.transformTo(
                    fail(join, "Stream to stream JOIN condition must contain time boundness predicate")
            );
            return;
        }

        BoundsExtractorVisitor visitor = new BoundsExtractorVisitor(join, watermarkedFields.f2());
        predicate.accept(visitor);

        if (!visitor.isValid) {
            call.transformTo(
                    fail(join, "Stream to stream JOIN condition must contain time boundness predicate")
            );
            return;
        }
        // endregion

        Map<OptUtils.RelField, Map<OptUtils.RelField, Long>> postponeMap = new HashMap<>();

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

        Map<OptUtils.RelField, Long> leftBoundMap = new HashMap<>();
        leftBoundMap.put(llRelation, visitor.leftBound.f2());
        postponeMap.put(lrRelation, leftBoundMap);

        Map<OptUtils.RelField, Long> rightBoundMap = new HashMap<>();
        rightBoundMap.put(rlRelation, visitor.rightBound.f2());
        postponeMap.put(rrRelation, rightBoundMap);

        Tuple2<Map<Integer, Integer>, Map<Integer, Integer>> wmFieldsMapping =
                joinToInputFieldsWmKeysMapping(join, watermarkedFields);

        call.transformTo(
                new StreamToStreamJoinPhysicalRel(
                        join.getCluster(),
                        join.getTraitSet().replace(PHYSICAL),
                        RelRule.convert(call.rel(1), call.rel(1).getTraitSet().replace(PHYSICAL)),
                        RelRule.convert(call.rel(2), call.rel(2).getTraitSet().replace(PHYSICAL)),
                        join.getCondition(),
                        join.getJoinType(),
                        watermarkedFields.f0(),
                        watermarkedFields.f1(),
                        wmFieldsMapping.f0(),
                        wmFieldsMapping.f1(),
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

    /**
     * Extracts all watermarked fields represented in JOIN relation row type.
     *
     * @param join        root relation to start search from
     * @param leftFields
     * @param rightFields
     * @return left, right input and joint watermarked fields from rel tree
     */
    private Tuple3<WatermarkedFields, WatermarkedFields, WatermarkedFields> watermarkedFields(
            JoinLogicalRel join, WatermarkedFields leftFields, WatermarkedFields rightFields) {
        Map<Integer, RexInputRef> leftPropsByIndex = leftFields.getPropertiesByIndex();
        Map<Integer, RexInputRef> leftResultInputRefMap = new HashMap<>();
        Map<Integer, RexInputRef> rightPropsByIndex = rightFields.getPropertiesByIndex();
        Map<Integer, RexInputRef> rightResultInputRefMap = new HashMap<>();

        for (Integer key : leftPropsByIndex.keySet()) {
            RelDataTypeField leftField = join.getLeft().getRowType().getFieldList().get(key);
            for (RelDataTypeField field : join.getRowType().getFieldList()) {
                if (field.getType().equals(leftField.getType()) && field.getName().equals(leftField.getName())) {
                    leftResultInputRefMap.put(field.getIndex(),
                            join.getCluster().getRexBuilder().makeInputRef(leftField.getType(), field.getIndex()));
                }
            }
        }

        for (Integer key : rightPropsByIndex.keySet()) {
            RelDataTypeField leftField = join.getRight().getRowType().getFieldList().get(key);
            for (RelDataTypeField field : join.getRowType().getFieldList()) {
                if (field.getType().equals(leftField.getType()) && field.getName().equals(leftField.getName())) {
                    rightResultInputRefMap.put(field.getIndex(),
                            join.getCluster().getRexBuilder().makeInputRef(leftField.getType(), field.getIndex()));
                }
            }
        }

        return Tuple3.tuple3(leftFields, rightFields, join(leftResultInputRefMap, rightResultInputRefMap));
    }

    /**
     * Creates mapping between join rel watermarked fields indices
     * and separately left and right input rel watermarked fields indices.
     * It is required by processor for enlighten of row timestamp extraction.
     * For more information :{@link StreamToStreamJoinP#clearExpiredItemsInBuffer}
     */
    @SuppressWarnings("JavadocReference")
    private Tuple2<Map<Integer, Integer>, Map<Integer, Integer>> joinToInputFieldsWmKeysMapping(
            JoinLogicalRel join,
            Tuple3<WatermarkedFields, WatermarkedFields, WatermarkedFields> watermarkedFieldsTuple) {
        Map<Integer, Integer> leftToJoinWmFieldsMapping = new HashMap<>();
        Map<Integer, Integer> rightToJoinWmFieldsMapping = new HashMap<>();

        List<RelDataTypeField> leftFieldsList = join.getLeft().getRowType().getFieldList();
        List<RelDataTypeField> rightFieldsList = join.getRight().getRowType().getFieldList();
        List<RelDataTypeField> joinFieldsList = join.getRowType().getFieldList();
        Set<Integer> joinedWmKeys = watermarkedFieldsTuple.f2().getPropertiesByIndex().keySet();

        for (Integer idx : joinedWmKeys) {
            // TODO: optimize, only TIMESTAMP allowed?
            extractMapping(leftFieldsList, joinFieldsList, leftToJoinWmFieldsMapping, idx);
            extractMapping(rightFieldsList, joinFieldsList, rightToJoinWmFieldsMapping, idx);
        }

        return tuple2(leftToJoinWmFieldsMapping, rightToJoinWmFieldsMapping);
    }

    private void extractMapping(
            List<RelDataTypeField> inputFieldsList,
            List<RelDataTypeField> joinFieldsList,
            Map<Integer, Integer> inputToJoinWmFieldsMapping,
            int idx) {
        for (int j = 0; j < inputFieldsList.size(); ++j) {
            if (inputFieldsList.get(j).getName().equals(joinFieldsList.get(idx).getName()) &&
                    inputFieldsList.get(j).getType().equals(joinFieldsList.get(idx).getType())) {
                inputToJoinWmFieldsMapping.put(j, idx);
                return;
            }
        }
    }

    @SuppressWarnings("CheckStyle")
    private static final class BoundsExtractorVisitor extends RexVisitorImpl<Void> {
        private final Join joinRel;
        private final WatermarkedFields watermarkedFields;

        public Tuple3<RelDataTypeField, RelDataTypeField, Long> leftBound = null;
        public Tuple3<RelDataTypeField, RelDataTypeField, Long> rightBound = null;
        public boolean isValid = true;

        private BoundsExtractorVisitor(JoinLogicalRel joinRel, WatermarkedFields watermarkedFields) {
            super(true);
            this.joinRel = joinRel;
            this.watermarkedFields = watermarkedFields;
        }

        // Note: we're expecting equality or `a BETWEEN b and c` call here. Anything else will be rejected.
        @Override
        public Void visitCall(RexCall call) {
            switch (call.getKind()) {
                case AND:
                    return super.visitCall(call);
                case EQUALS:
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
         * Extract time boundness from GTE/LTE calls
         * represented as `$ref1 >= $ref2 + constant1`
         * or `$ref1 <= $ref2 + constant2`.
         *
         * @return Tuple [ref1_index, ref2_index, bound]
         */
        private Tuple3<RelDataTypeField, RelDataTypeField, Long> extractBoundFromComparisonCall(
                RexCall call,
                WatermarkedFields wmFields,
                boolean isLeft
        ) {
            assert call.getOperands().get(0) instanceof RexInputRef;
            RexInputRef leftOp = (RexInputRef) call.getOperands().get(0);
            RexNode rightOp = call.getOperands().get(1);

            RelDataTypeField leftField = joinRel.getRowType().getFieldList().get(leftOp.getIndex());

            Tuple3<RelDataTypeField, RelDataTypeField, Long> boundToReturn = null;

            if (rightOp instanceof RexCall) {
                Tuple2<RelDataTypeField, Long> timeBound = extractTimeBoundFromAddition((RexCall) rightOp, wmFields);
                boundToReturn = isValid ? tuple3(leftField, timeBound.f0(), timeBound.f1()) : null;
            } else if (rightOp instanceof RexInputRef) {
                RexInputRef rightIRef = (RexInputRef) rightOp;
                RelDataTypeField rightField = joinRel.getRowType().getFieldList().get((rightIRef).getIndex());
                boundToReturn = tuple3(leftField, rightField, 0L);
            } else {
                isValid = false;
            }

            /*
             It's needed to enlighten mathematical representation for group of canonical time-bound non-equations.
             Read more: https://github.com/Fly-Style/hazelcast/blob/feat/5.2/stream-to-stream-join-design/docs/design/sql/15-stream-to-stream-join.md#implementation-of-multiple-watermarks-in-the-join-processor
            */
            if (isValid && isLeft) {
                boundToReturn = tuple3(boundToReturn.f1(), boundToReturn.f0(), -boundToReturn.f2());
            }

            return boundToReturn;
        }

        /**
         * Extract time boundness from PLUS/MINUS calls represented as `$ref + constant`.
         */
        private Tuple2<RelDataTypeField, Long> extractTimeBoundFromAddition(
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
                if (!watermarkedFields.getPropertiesByIndex().containsValue(inputRef)) {
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
