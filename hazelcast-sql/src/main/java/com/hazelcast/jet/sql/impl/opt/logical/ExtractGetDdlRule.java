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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.google.common.collect.ImmutableList;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastObjectType;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

@Value.Enclosing
public class ExtractGetDdlRule extends RelRule<RelRule.Config> {

    public static final RelOptRule INSTANCE = new ExtractGetDdlRule(Config.DEFAULT);

    private GetDdlFunctionFinder finder;

    @Value.Immutable
    public interface Config extends RelRule.Config {
        ExtractGetDdlRule.Config DEFAULT = ImmutableExtractGetDdlRule.Config.builder()
                .description(ExtractGetDdlRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .oneInput(b1 -> b1
                                .operand(RelNode.class)
                                .noInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new ExtractGetDdlRule(this);
        }
    }

    public ExtractGetDdlRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        finder = new GetDdlFunctionFinder();
        Calc rel = call.rel(0);
        rel.accept(finder);
        if (finder.multipleEntries) {
            throw QueryException.error("Multiple GET_DDL in single query are not allowed");
        }
        return finder.found;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        checkNotNull(finder);

        Calc calc = call.rel(0);

        List<RexNode> getDdlOperands = calc.getProgram().expandList(finder.functionOperands);
        LogicalGetDdlRel getDdlRel;
        if (getDdlOperands.stream().allMatch(rex -> rex instanceof RexLiteral)) {
            HazelcastObjectType.Field namespaceField = new HazelcastObjectType.Field(
                    "namespace",
                    0,
                    HazelcastTypeFactory.INSTANCE.createSqlType(VARCHAR));

            HazelcastObjectType.Field objectNameField = new HazelcastObjectType.Field(
                    "objectName",
                    1,
                    HazelcastTypeFactory.INSTANCE.createSqlType(VARCHAR));

            HazelcastObjectType.Field schemaField = new HazelcastObjectType.Field(
                    "schema",
                    2,
                    HazelcastTypeFactory.INSTANCE.createSqlType(VARCHAR));

            RelDataType inputRowType = new RelRecordType(
                    getDdlOperands.size() == 2
                            ? Arrays.asList(namespaceField, objectNameField)
                            : Arrays.asList(namespaceField, objectNameField, schemaField)
            );


            Values newValues = new LogicalValues(
                    calc.getCluster(),
                    calc.getTraitSet(),
                    inputRowType,
                    ImmutableList.of(
                            ImmutableList.copyOf(
                                    getDdlOperands.stream()
                                            .map(rex -> ((RexLiteral) rex))
                                            .collect(toList())))
            );
            getDdlRel = new LogicalGetDdlRel(calc.getCluster(), calc.getTraitSet(), newValues);
        } else {
            getDdlRel = new LogicalGetDdlRel(calc.getCluster(), calc.getTraitSet(), calc.getInput());
        }

        RexProgramBuilder rpb = buildNewProjection(calc, getDdlRel, call.rel(1));
        call.transformTo(
                new LogicalCalc(calc.getCluster(), calc.getTraitSet(), calc.getHints(), getDdlRel, rpb.getProgram())
        );
    }

    /**
     * Erase GET_DDL function with parameters and rewrite projection rex program for {@link Calc}.
     * <p>
     * Since GET_DDL function is moved from Expression to separate relation {@link LogicalGetDdlRel},
     * we need to correctly rewrite remaining projection without GET_DDL call and its arguments.
     */
    private static RexProgramBuilder buildNewProjection(Calc calc, LogicalGetDdlRel getDdlRel, RelNode input) {
        RexProgram program = calc.getProgram();
        List<RexNode> exprList = new ArrayList<>(program.getExprList());
        List<RexNode> projList = new ArrayList<>(program.getProjectList());

        // If query doesn't have FROM clause, we should manually change 'input' type.
        if (input instanceof Values && !input.getRowType().equals(getDdlRel.getRowType())) {
            exprList.set(0, new RexInputRef(0, getDdlRel.getRowType()));
        }

        // Find GET_DDL call
        int idx = 0;
        for (RexNode expr : exprList) {
            if (expr instanceof RexCall && ((RexCall) expr).getOperator() == HazelcastSqlOperatorTable.GET_DDL) {
                break;
            }
            ++idx;
        }
        // Erase GET_DDL call and its parameters.
        RexCall getDdlCall = (RexCall) exprList.get(idx);
        int shiftSize = getDdlCall.getOperands().size() + 1;
        for (int i = 0; i < shiftSize; ++i) {
            exprList.remove(idx - i);
        }

        // Shift dependent
        List<RexNode> expressions = new RexShiftBackShuttle(shiftSize).visitList(exprList);
        List<RexNode> projections = new RexShiftBackShuttle(shiftSize).visitList(projList);
        RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        return RexProgramBuilder.create(
                rexBuilder,
                calc.getRowType(),
                expressions,
                projections,
                null,   // TODO: any conditions may be applied here?
                calc.getProgram().getOutputRowType(),
                true,
                null
        );
    }

    @SuppressWarnings("checkstyle:VisibilityModifier")
    static class GetDdlFunctionFinder extends RexShuttle {
        public List<RexNode> functionOperands;
        public boolean found;
        public boolean multipleEntries;

        @Override
        public RexNode visitCall(RexCall call) {
            if (call.getOperator() == HazelcastSqlOperatorTable.GET_DDL) {
                if (found) {
                    multipleEntries = true;
                    return super.visitCall(call);
                }
                functionOperands = call.getOperands();
                found = true;
            }
            return super.visitCall(call);
        }
    }

    static class RexShiftBackShuttle extends RexShuttle {
        private final int offset;

        RexShiftBackShuttle(int offset) {
            this.offset = offset;
        }

        @Override
        public RexNode visitLocalRef(RexLocalRef input) {
            return new RexLocalRef(input.getIndex() - offset, input.getType());
        }
    }
}
