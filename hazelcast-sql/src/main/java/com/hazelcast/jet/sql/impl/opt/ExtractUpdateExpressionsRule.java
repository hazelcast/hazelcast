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

package com.hazelcast.jet.sql.impl.opt;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.HazelcastRexNode.wrap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

/**
 * A rule that:<ul>
 *     <li>extracts unsupported source expressions from an UPDATE stmt into a
 *         Calc
 *     <li>moves PK fields to be the initial fields of the row type
 * </ul>
 */
@Value.Enclosing
public class ExtractUpdateExpressionsRule extends RelRule<RelRule.Config> {

    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    @Value.Immutable
    interface Config extends RelRule.Config {
        RelRule.Config DEFAULT = ImmutableExtractUpdateExpressionsRule.Config.builder()
                .description(ExtractUpdateExpressionsRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModify.class)
                        .predicate(TableModify::isUpdate)
                        .anyInputs()
                ).build();

        @Override
        default RelOptRule toRule() {
            return new ExtractUpdateExpressionsRule(this);
        }
    }

    public ExtractUpdateExpressionsRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModify update = call.rel(0);
        assert update.getSourceExpressionList() != null;
        assert update.isUpdate();

        HazelcastTable hzTable = OptUtils.extractHazelcastTable(update);
        SqlConnector sqlConnector = getJetSqlConnector(hzTable.getTarget());

        RexBuilder rexBuilder = call.builder().getRexBuilder();
        List<RexNode> projections = new ArrayList<>();
        // add identity projection
        for (int i = 0; i < update.getInput().getRowType().getFieldCount(); i++) {
            projections.add(rexBuilder.makeInputRef(update.getInput(), i));
        }

        boolean expressionsAdded = false;

        // add unsupported source expressions to the projection
        List<RexNode> sourceExpressions = new ArrayList<>(update.getSourceExpressionList());
        for (int i = 0; i < sourceExpressions.size(); i++) {
            RexNode expr = sourceExpressions.get(i);
            // input ref must be supported by every connector
            if (expr instanceof RexInputRef || expr instanceof RexDynamicParam) {
                continue;
            }
            if (!sqlConnector.supportsExpression(wrap(expr))) {
                expressionsAdded = true;
                sourceExpressions.set(i, rexBuilder.makeInputRef(expr.getType(), projections.size()));
                projections.add(expr);
            }
        }

        // move PK fields to the beginning of the row type
        Permutation permutation = permutePkFieldsToBeginning(sqlConnector.getPrimaryKey(hzTable.getTarget()),
                update.getInput().getRowType());

        // do the transformation, if needed
        if (expressionsAdded || !permutation.isIdentity()) {
            // permute the projections
            // `set` called for the side effect of appending an identity projection for the remainder of projected fields
            permutation.set(projections.size() - 1, projections.size() - 1, true);
            projections = Mappings.apply(permutation, projections);

            RelNode newProject = call.builder()
                    .push(update.getInput())
                    .project(projections)
                    .build();

            // permute the input references in sourceExpressions
            if (!permutation.isIdentity()) {
                RexPermuteInputsShuttle shuttle = new RexPermuteInputsShuttle(permutation, newProject);
                sourceExpressions.replaceAll(expr -> expr.accept(shuttle));
            }

            LogicalTableModify newUpdate = new LogicalTableModify(update.getCluster(), update.getTraitSet(), update.getTable(),
                    update.getCatalogReader(), newProject, update.getOperation(), update.getUpdateColumnList(),
                    sourceExpressions, update.isFlattened());

            call.transformTo(newUpdate);
        }
    }

    private Permutation permutePkFieldsToBeginning(List<String> primaryKey, RelDataType rowType) {
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        Permutation result = new Permutation(rowType.getFieldCount());

        outer:
        for (int i = 0; i < primaryKey.size(); i++) {
            for (int j = 0; j < fieldList.size(); j++) {
                if (fieldList.get(j).getName().equals(primaryKey.get(i))) {
                    result.set(j, i);
                    continue outer;
                }
            }
            throw new RuntimeException("PK field not found in the input row: " + primaryKey.get(i));
        }

        return result;
    }
}
