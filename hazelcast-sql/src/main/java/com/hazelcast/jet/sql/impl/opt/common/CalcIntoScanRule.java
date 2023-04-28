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

package com.hazelcast.jet.sql.impl.opt.common;

import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.opt.FullScan;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.HazelcastRexNode.wrap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static org.apache.calcite.rex.RexUtil.EXECUTOR;

/**
 * Logical rule that pushes supported expressions from a {@link Calc} down into
 * a {@link TableScan} to allow for constrained scans. See {@link
 * HazelcastTable} for more information about constrained scans.
 * <p>
 * Before:
 * <pre>
 * Calc[filter=expSupported AND expUnsupported]
 *     TableScan[table[filter=exp1]]
 * </pre>
 * After:
 * <pre>
 * Calc[filter=expUnsupported]
 *     TableScan[table[filter=expSupported AND exp1]]
 * </pre>
 *
 * If we cannot push down the entire filter, projections aren't merged down.
 * They are also not merged if any of the Calc's projections contains an
 * unsupported expression; splitting the supported an unsupported projections is
 * not implemented.
 */
@Value.Enclosing
public final class CalcIntoScanRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        CalcIntoScanRule.Config DEFAULT = ImmutableCalcIntoScanRule.Config.builder()
                .description(CalcIntoScanRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Calc.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(FullScan.class)
                                .noInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new CalcIntoScanRule(this);
        }
    }

    public static final CalcIntoScanRule INSTANCE = new CalcIntoScanRule(Config.DEFAULT);

    private CalcIntoScanRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // we don't merge projections. Refuse to match the rule if the scan's projection isn't identity.
        FullScan scan = call.rel(1);
        HazelcastTable table = OptUtils.extractHazelcastTable(scan);
        Permutation permutation = Project.getPermutation(table.getTarget().getFieldCount(), table.getProjects());
        return permutation != null && permutation.isIdentity();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Calc calc = call.rel(0);
        FullScan scan = call.rel(1);

        HazelcastTable table = OptUtils.extractHazelcastTable(scan);
        SqlConnector sqlConnector = getJetSqlConnector(table.getTarget());
        RexProgram calcProgram = calc.getProgram();
        assert scan.getConvention() == LOGICAL;
        HazelcastTable newTable = table;
        RexNode remainingFilter = null;
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RexSimplify rexSimplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, EXECUTOR);

        // Merge filters first. Filters are evaluated before the projection; if some filter isn't supported, we
        // can't push the projection.
        if (calcProgram.getCondition() != null) {
            RexNode calcFilter = calcProgram.expandLocalRef(calcProgram.getCondition());
            RexNode scanFilter = table.getFilter();

            List<RexNode> supportedParts = new ArrayList<>();
            List<RexNode> unsupportedParts = new ArrayList<>();
            for (RexNode expr : RelOptUtil.conjunctions(calcFilter)) {
                (sqlConnector.supportsExpression(wrap(expr)) ? supportedParts : unsupportedParts)
                        .add(expr);
            }

            supportedParts.add(scanFilter);
            RexNode simplifiedCondition = rexSimplify.simplifyFilterPredicates(supportedParts);
            if (simplifiedCondition == null) {
                simplifiedCondition = rexBuilder.makeLiteral(false);
            }
            newTable = newTable.withFilter(simplifiedCondition);
            remainingFilter = RexUtil.composeConjunction(rexBuilder, unsupportedParts, true);
        }

        List<RexNode> newProjects = calcProgram.expandList(calcProgram.getProjectList());
        boolean projectsSupported = newProjects.stream()
                .map(HazelcastRexNode::wrap)
                .allMatch(sqlConnector::supportsExpression);

        if (projectsSupported && remainingFilter == null) {
            newTable = newTable.withProject(newProjects, calcProgram.getOutputRowType());
            newProjects = null;
        }

        HazelcastRelOptTable convertedTable = OptUtils.createRelTable(
                (HazelcastRelOptTable) scan.getTable(),
                newTable,
                scan.getCluster().getTypeFactory()
        );

        FullScanLogicalRel newScan = new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                convertedTable,
                scan.lagExpression(),
                OptUtils.getTargetField(calcProgram, scan.watermarkedColumnIndex())
        );

        if (newProjects != null) {
            RexProgramBuilder progBuilder = new RexProgramBuilder(convertedTable.getRowType(), rexBuilder);
            for (Pair<RexNode, String> pair : Pair.zip(newProjects, calc.getRowType().getFieldNames())) {
                progBuilder.addProject(pair.left, pair.right);
            }
            if (remainingFilter != null) {
                progBuilder.addCondition(remainingFilter);
            }
            Calc newCalc = calc.copy(calc.getTraitSet(), newScan, progBuilder.getProgram());
            call.transformTo(newCalc);
        } else {
            assert remainingFilter == null;
            call.transformTo(newScan);
        }
    }
}
