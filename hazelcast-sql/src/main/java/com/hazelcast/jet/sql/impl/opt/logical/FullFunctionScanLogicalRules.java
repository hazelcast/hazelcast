/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.JetDynamicTableFunction;
import com.hazelcast.jet.sql.impl.schema.JetSpecificTableFunction;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.type.QueryDataType;
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

final class FullFunctionScanLogicalRules {

    static final RelOptRule SPECIFIC_FUNCTION_INSTANCE = new ConverterRule(
            LogicalTableFunctionScan.class, scan -> extractSpecificFunction(scan) != null,
            Convention.NONE, Convention.NONE,
            FullFunctionScanLogicalRules.class.getSimpleName() + "(Specific)"
    ) {
        @Override
        public RelNode convert(RelNode rel) {
            LogicalTableFunctionScan scan = (LogicalTableFunctionScan) rel;

            return OptUtils.createLogicalScan(scan.getCluster(), extractTable(scan));
        }

        private HazelcastTable extractTable(LogicalTableFunctionScan scan) {
            JetSpecificTableFunction specificFunction = extractSpecificFunction(scan);
            List<RexNode> operands = ((RexCall) scan.getCall()).getOperands();
            RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(
                    FailingFieldTypeProvider.INSTANCE,
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
            FullFunctionScanLogicalRules.class.getSimpleName() + "(Dynamic)"
    ) {
        @Override
        public RelNode convert(RelNode rel) {
            LogicalTableFunctionScan scan = (LogicalTableFunctionScan) rel;

            return OptUtils.createLogicalScan(scan.getCluster(), extractTable(scan));
        }

        private HazelcastTable extractTable(LogicalTableFunctionScan scan) {
            JetDynamicTableFunction dynamicFunction = extractDynamicFunction(scan);
            return dynamicFunction.toTable(scan.getRowType());
        }
    };

    private FullFunctionScanLogicalRules() {
    }

    private static JetSpecificTableFunction extractSpecificFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof JetSpecificTableFunction)) {
            return null;
        }
        return (JetSpecificTableFunction) call.getOperator();
    }

    private static JetDynamicTableFunction extractDynamicFunction(LogicalTableFunctionScan scan) {
        if (scan == null || !(scan.getCall() instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) scan.getCall();

        if (!(call.getOperator() instanceof JetDynamicTableFunction)) {
            return null;
        }
        return (JetDynamicTableFunction) call.getOperator();
    }

    private static final class FailingFieldTypeProvider implements PlanNodeFieldTypeProvider {

        private static final FailingFieldTypeProvider INSTANCE = new FailingFieldTypeProvider();

        @Override
        public QueryDataType getType(int index) {
            throw new IllegalStateException("The operation should not be called.");
        }
    }
}
