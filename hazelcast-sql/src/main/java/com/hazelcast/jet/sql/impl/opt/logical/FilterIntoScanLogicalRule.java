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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Arrays.asList;

/**
 * Logical rule that pushes down a {@link Filter} into a {@link TableScan} to allow for constrained scans.
 * See {@link HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * LogicalFilter[filter=exp1]
 *     LogicalScan[table[filter=exp2]]
 * </pre>
 * After:
 * <pre>
 * LogicalScan[table[filter=exp1 AND exp2]]
 * </pre>
 */
public final class FilterIntoScanLogicalRule extends RelRule<Config> implements TransformationRule {

    private static final Config CONFIG = Config.EMPTY
            .withDescription(FilterIntoScanLogicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0
                    .operand(Filter.class)
                    .trait(LOGICAL)
                    .inputs(b1 -> b1
                            .operand(FullScanLogicalRel.class).anyInputs()));

    public static final RelOptRule INSTANCE = new FilterIntoScanLogicalRule(CONFIG);

    private FilterIntoScanLogicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

        HazelcastTable table = OptUtils.extractHazelcastTable(scan);

        List<RexNode> projection = table.getProjects();
        RexNode existingCondition = table.getFilter();
        RexNode condition = filter.getCondition();

        RexNode convertedCondition = RexUtil.apply(new ProjectFieldVisitor(projection), new RexNode[]{condition})[0];
        if (existingCondition != null) {
            convertedCondition = RexUtil.composeConjunction(
                    scan.getCluster().getRexBuilder(),
                    asList(existingCondition, convertedCondition),
                    true
            );
        }

        RelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                table.withFilter(convertedCondition),
                scan.getCluster().getTypeFactory()
        );

        FullScanLogicalRel rel = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                scan.eventTimePolicyProvider(),
                scan.watermarkedColumnIndex()
        );
        call.transformTo(rel);
    }

    private static class ProjectFieldVisitor implements RexVisitor<RexNode> {

        private final List<RexNode> projection;

        protected ProjectFieldVisitor(List<RexNode> projection) {
            this.projection = projection;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return projection.get(inputRef.getIndex());
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef localRef) {
            return localRef;
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }

        @Override
        public RexNode visitOver(RexOver over) {
            throw new UnsupportedOperationException("OVER statement is not supported.");
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            List<RexNode> newOperands = new ArrayList<>(call.getOperands().size());
            for (RexNode operand : call.operands) {
                newOperands.add(operand.accept(this));
            }
            return call.clone(call.type, newOperands);
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        @Override
        public RexNode visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexNode expr = fieldAccess.getReferenceExpr();
            RexNode newOperand = expr.accept(this);
            if (newOperand != fieldAccess.getReferenceExpr()) {
                throw new RuntimeException("replacing partition key not supported");
            }
            return fieldAccess;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            List<RexNode> newOperands = new ArrayList<>(subQuery.operands.size());
            for (RexNode operand : subQuery.operands) {
                newOperands.add(operand.accept(this));
            }
            return subQuery.clone(subQuery.type, newOperands);
        }

        @Override
        public RexNode visitTableInputRef(RexTableInputRef ref) {
            return ref;
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return fieldRef;
        }
    }
}
