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

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.aggregate.function.HazelcastWindowTableFunction;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastDynamicTableFunction;
import com.hazelcast.jet.sql.impl.schema.HazelcastSpecificTableFunction;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

final class FunctionLogicalRules {

    static final RelOptRule SPECIFIC_FUNCTION_INSTANCE = new ConverterRule(
            LogicalTableFunctionScan.class, scan -> extractSpecificFunction(scan) != null,
            Convention.NONE, Convention.NONE,
            FunctionLogicalRules.class.getSimpleName() + "(Specific)"
    ) {
        @Override
        public RelNode convert(RelNode rel) {
            LogicalTableFunctionScan scan = (LogicalTableFunctionScan) rel;

            return OptUtils.createLogicalScan(scan.getCluster(), extractTable(scan));
        }

        private HazelcastTable extractTable(LogicalTableFunctionScan scan) {
            HazelcastSpecificTableFunction specificFunction = extractSpecificFunction(scan);
            List<RexNode> operands = ((RexCall) scan.getCall()).getOperands();
            RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(
                    PlanNodeFieldTypeProvider.FAILING_FIELD_TYPE_PROVIDER,
                    ((HazelcastRelOptCluster) scan.getCluster()).getParameterMetadata()
            );
            List<Expression<?>> argumentExpressions = IntStream.range(0, specificFunction.getOperandCountRange().getMax())
                    .mapToObj(index -> index < operands.size()
                            ? (Expression<?>) operands.get(index).accept(visitor)
                            : null
                    ).collect(toList());
            return specificFunction.toTable(argumentExpressions);
        }
    };

    static final RelOptRule DYNAMIC_FUNCTION_INSTANCE = new ConverterRule(
            LogicalTableFunctionScan.class, scan -> extractDynamicFunction(scan) != null,
            Convention.NONE, Convention.NONE,
            FunctionLogicalRules.class.getSimpleName() + "(Dynamic)"
    ) {
        @Override
        public RelNode convert(RelNode rel) {
            LogicalTableFunctionScan scan = (LogicalTableFunctionScan) rel;

            return OptUtils.createLogicalScan(scan.getCluster(), extractTable(scan));
        }

        private HazelcastTable extractTable(LogicalTableFunctionScan scan) {
            HazelcastDynamicTableFunction dynamicFunction = extractDynamicFunction(scan);
            return dynamicFunction.toTable(scan.getRowType());
        }
    };

    static final RelOptRule WINDOW_FUNCTION_INSTANCE = new ConverterRule(
            LogicalTableFunctionScan.class, scan -> extractWindowFunction(scan) != null,
            Convention.NONE, Convention.NONE,
            FunctionLogicalRules.class.getSimpleName() + "(Window)"
    ) {
        @Override
        public RelNode convert(RelNode rel) {
            LogicalTableFunctionScan scan = (LogicalTableFunctionScan) rel;

            return new SlidingWindowLogicalRel(
                    scan.getCluster(),
                    OptUtils.toLogicalConvention(scan.getTraitSet()),
                    Util.toList(scan.getInputs(), OptUtils::toLogicalInput),
                    scan.getCall(),
                    scan.getElementType(),
                    scan.getRowType(),
                    scan.getColumnMappings()
            );
        }
    };

    private FunctionLogicalRules() {
    }

    private static HazelcastSpecificTableFunction extractSpecificFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof HazelcastSpecificTableFunction)) {
            return null;
        }
        return (HazelcastSpecificTableFunction) call.getOperator();
    }

    private static HazelcastDynamicTableFunction extractDynamicFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof HazelcastDynamicTableFunction)) {
            return null;
        }
        return (HazelcastDynamicTableFunction) call.getOperator();
    }

    private static HazelcastWindowTableFunction extractWindowFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof HazelcastWindowTableFunction)) {
            return null;
        }
        return (HazelcastWindowTableFunction) call.getOperator();
    }
}
