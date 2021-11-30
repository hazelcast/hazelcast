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
import com.hazelcast.jet.sql.impl.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER;
import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;

final class WatermarkRules {

    @SuppressWarnings("AnonInnerLength")
    static final RelOptRule IMPOSE_ORDER_INSTANCE = new ConverterRule(
            LogicalTableFunctionScan.class, scan -> extractImposeOrderFunction(scan) != null,
            Convention.NONE, LOGICAL,
            WatermarkRules.class.getSimpleName() + "(Impose Order)"
    ) {
        @Override
        public RelNode convert(RelNode rel) {
            LogicalTableFunctionScan function = (LogicalTableFunctionScan) rel;

            return new WatermarkLogicalRel(
                    function.getCluster(),
                    OptUtils.toLogicalConvention(function.getTraitSet()),
                    Iterables.getOnlyElement(Util.toList(function.getInputs(), OptUtils::toLogicalInput)),
                    toEventTimePolicyProvider(function)
            );
        }

        private FunctionEx<ExpressionEvalContext, EventTimePolicy<Object[]>> toEventTimePolicyProvider(
                LogicalTableFunctionScan function
        ) {
            int orderingColumnFieldIndex = orderingColumnFieldIndex(function);
            Expression<?> lagExpression = lagExpression(function);
            return context -> {
                long lagMs = WindowUtils.extractMillis(lagExpression, context);
                return EventTimePolicy.eventTimePolicy(
                        row -> WindowUtils.extractMillis(row[orderingColumnFieldIndex]),
                        (row, timestamp) -> row,
                        WatermarkPolicy.limitingLag(lagMs),
                        lagMs,
                        0,
                        EventTimePolicy.DEFAULT_IDLE_TIMEOUT
                );
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

    static final RelOptRule WATERMARK_INTO_SCAN_INSTANCE = new RelOptRule(
            operand(WatermarkLogicalRel.class, operand(FullScanLogicalRel.class, none())),
            WatermarkRules.class.getSimpleName() + "(Watermark Into Scan)"
    ) {
        @Override
        public void onMatch(RelOptRuleCall call) {
            WatermarkLogicalRel logicalWatermark = call.rel(0);
            FullScanLogicalRel logicalScan = call.rel(1);

            RelNode rel = new FullScanLogicalRel(
                    logicalWatermark.getCluster(),
                    logicalWatermark.getTraitSet(),
                    logicalScan.getTable(),
                    logicalWatermark.eventTimePolicyProvider()
            );
            call.transformTo(rel);
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
