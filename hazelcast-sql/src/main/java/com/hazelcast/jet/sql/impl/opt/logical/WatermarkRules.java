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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.google.common.collect.Iterables;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.aggregate.WindowUtils;
import com.hazelcast.jet.sql.impl.aggregate.function.ImposeOrderFunction;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Map;

import static com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMdWatermarkedFields.watermarkedFieldByIndex;
import static com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.operandJ;

final class WatermarkRules {

    /**
     * This rule converts IMPOSE_ORDER function call into
     * WatermarkLogicalRel. This rel is later pushed down into scan in
     * {@link #WATERMARK_INTO_SCAN_INSTANCE}.
     */
    @SuppressWarnings("AnonInnerLength")
    static final RelOptRule IMPOSE_ORDER_INSTANCE = new RelOptRule(
            operandJ(
                    LogicalTableFunctionScan.class,
                    Convention.NONE,
                    scan -> extractImposeOrderFunction(scan) != null,
                    none()
            ),
            WatermarkRules.class.getSimpleName() + "(Impose Order)"
    ) {
        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalTableFunctionScan scan = call.rel(0);

            int wmIndex = orderingColumnFieldIndex(scan);
            WatermarkLogicalRel wmRel = new WatermarkLogicalRel(
                    scan.getCluster(),
                    OptUtils.toLogicalConvention(scan.getTraitSet()),
                    Iterables.getOnlyElement(Util.toList(scan.getInputs(), OptUtils::toLogicalInput)),
                    toEventTimePolicyProvider(scan),
                    wmIndex);

            if (wmIndex < 0) {
                call.transformTo(wmRel);
                return;
            }
            WatermarkedFields watermarkedFields = watermarkedFieldByIndex(wmRel, wmIndex);
            if (watermarkedFields == null || watermarkedFields.isEmpty()) {
                call.transformTo(wmRel);
                return;
            }

            Map.Entry<Integer, RexNode> watermarkedField = watermarkedFields.findFirst();
            if (watermarkedField == null) {
                call.transformTo(wmRel);
                return;
            }

            DropLateItemsLogicalRel dropLateItemsRel = new DropLateItemsLogicalRel(
                    scan.getCluster(),
                    OptUtils.toLogicalConvention(scan.getTraitSet()),
                    wmRel,
                    watermarkedField.getValue()
            );
            call.transformTo(dropLateItemsRel);
        }

        private FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> toEventTimePolicyProvider(
                LogicalTableFunctionScan function
        ) {
            int orderingColumnFieldIndex = orderingColumnFieldIndex(function);
            Expression<?> lagExpression = lagExpression(function);
            return context -> {
                // todo [viliam] move this to CreateDagVisitor
                long lagMs = WindowUtils.extractMillis(lagExpression, context);
                return EventTimePolicy.eventTimePolicy(
                        row -> WindowUtils.extractMillis(row.get(orderingColumnFieldIndex)),
                        (row, timestamp) -> row,
                        WatermarkPolicy.limitingLag(lagMs),
                        lagMs,
                        0,
                        EventTimePolicy.DEFAULT_IDLE_TIMEOUT);
            };
        }

        private int orderingColumnFieldIndex(LogicalTableFunctionScan function) {
            return ((RexInputRef) ((RexCall) extractOperand(function, 1)).getOperands().get(0)).getIndex();
        }

        private Expression<?> lagExpression(LogicalTableFunctionScan function) {
            QueryParameterMetadata parameterMetadata = ((HazelcastRelOptCluster) function.getCluster()).getParameterMetadata();
            RexToExpressionVisitor visitor = new RexToExpressionVisitor(FAILING_FIELD_TYPE_PROVIDER, parameterMetadata);
            return extractOperand(function, 2).accept(visitor);
        }
    };

    /**
     * Push down {@link WatermarkLogicalRel} into {@link
     * FullScanLogicalRel}, if one is its input.
     */
    static final RelOptRule WATERMARK_INTO_SCAN_INSTANCE = new RelOptRule(
            operand(WatermarkLogicalRel.class, operand(FullScanLogicalRel.class, none())),
            WatermarkRules.class.getSimpleName() + "(Watermark Into Scan)"
    ) {
        @Override
        public void onMatch(RelOptRuleCall call) {
            WatermarkLogicalRel logicalWatermark = call.rel(0);
            FullScanLogicalRel logicalScan = call.rel(1);

            FullScanLogicalRel scan = new FullScanLogicalRel(
                    logicalWatermark.getCluster(),
                    logicalWatermark.getTraitSet(),
                    logicalScan.getTable(),
                    logicalWatermark.eventTimePolicyProvider(),
                    logicalWatermark.watermarkedColumnIndex()
            );
            call.transformTo(scan);
        }
    };

    private WatermarkRules() {
    }

    private static ImposeOrderFunction extractImposeOrderFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof ImposeOrderFunction)) {
            return null;
        }
        return (ImposeOrderFunction) call.getOperator();
    }

    private static RexNode extractOperand(LogicalTableFunctionScan function, int index) {
        return ((RexCall) function.getCall()).getOperands().get(index);
    }
}
