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

import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.opt.ExtractUpdateExpressionsRule;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.common.CalcIntoScanRule;
import com.hazelcast.jet.sql.impl.validate.HazelcastSqlValidator;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.sql.SqlUpdate;
import org.immutables.value.Value;

import static java.util.Objects.requireNonNull;

/**
 * A rule to match a TableModify[operation=update], whose input is other than
 * Scan - this is handled by {@link UpdateWithScanLogicalRule}.
 *
 * <h3>Overall description of the logical UPDATE/DELETE rules</h3>
 *
 * <ul>
 *     <li>{@link UpdateWithScanLogicalRule}
 *     <li>{@link UpdateNoScanLogicalRule}
 *     <li>{@link DeleteWithScanLogicalRule}
 *     <li>{@link DeleteNoScanLogicalRule}
 *     <li>{@link ExtractUpdateExpressionsRule}
 * </ul>
 *
 * A {@link TableModify} in Calcite has an input which is supposed to
 * (putatively) supply the input rows for the operation. However, various
 * connectors need different input or no input in various cases: <ol>
 *
 *     <li>Most connectors are able to delete/update rows according to a
 *     predicate. In this case no input is needed, the connector can execute the
 *     operation directly.
 *
 *     <li>Even though the connector is able to modify according to a predicate,
 *     not all expressions are supported. For example, the remote `TO_CHAR`
 *     function might be subtly different than our implementation. Typically
 *     simple operations are supported, such as `=` or `IS NULL`.
 *
 *     <li>Some connectors can delete/update rows only according to a primary
 *     key. Currently it's only the {@link com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector}. So a scan for
 *     affected rows must be performed separately.
 *
 * </ol>
 *
 * The connectors communicate their abilities using {@link
 * SqlConnector#dmlSupportsPredicates()} and {@link
 * SqlConnector#supportsExpression(HazelcastRexNode)} methods.
 * <p>
 * After sql-to-rel conversion, the plan looks like this:
 * <pre>
 *     -TableModify
 *     --Calc
 *     ---Scan
 * </pre>
 *
 * The Scan finds all the rows, Calc filters them according to the WHERE clause
 * in the DML statement, and also calculates the expressions needed for the
 * updated columns (e.g. when there's `SET foo=bar + 1`, it calculates the `bar
 * + 1` expression). The TableModify then executes the operation. What is weird
 * that for UPDATE, TableModify's source expressions don't contain input
 * references to the expression results from Calc, but are repeated. It might
 * seem that we can remove the adding of source expressions in {@link
 * HazelcastSqlValidator#createSourceSelectForUpdate(SqlUpdate)}, but Calcite
 * validation fails without that. Instead, we later use the {@link
 * ExtractUpdateExpressionsRule}, which pushes down unsupported expressions from
 * the TableModify a Project node, if they're not supported, and also makes sure
 * the PK fields are the initial fields, which is part of our contract in {@link
 * SqlConnector#updateProcessor} and {@link SqlConnector#deleteProcessor}.
 * <p>
 * We also rely on {@link CalcIntoScanRule} for potentially eliminating the Calc
 * entirely. If the Calc was eliminated, the {@link UpdateWithScanLogicalRule}
 * applies. It can lead to one of three transforms: <ol>
 *
 *     <li>key-based imap operation converted to {@link
 *     UpdateByKeyMapLogicalRel}
 *
 *     <li>{@link UpdateLogicalRel} without an input, if all expressions in the
 *     scan are supported
 *
 *     <li>{@link UpdateLogicalRel} with an input otherwise
 *
 * </ol>
 *
 * If the Calc wasn't eliminated, the {@link UpdateNoScanLogicalRule} rule will
 * simply convert the TableModify to {@link UpdateLogicalRel} with the same
 * input.
 * <p>
 * It works analogically for the DELETE rules, except that it's simpler because
 * they don't contain `sourceExpressions`.
 */
@Value.Enclosing
class UpdateNoScanLogicalRule extends RelRule<RelRule.Config> {

    static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    @Value.Immutable
    interface Config extends RelRule.Config {
        RelRule.Config DEFAULT = ImmutableUpdateNoScanLogicalRule.Config.builder()
                .description(UpdateNoScanLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModifyLogicalRel.class)
                         .predicate(TableModify::isUpdate)
                         .inputs(b1 -> b1.operand(RelNode.class)
                                 // no support for UPDATE FROM SELECT case (which is not yet implemented)
                                 // once joins are there we need to create complementary rule
                                 //
                                 // Calcite replaces the table scan with empty VALUES when the WHERE clause
                                 // is always false
                                 // i.e. '... WHERE __key = 1 AND __key = 2'
                                 .predicate(r -> !(r instanceof Values) && !(r instanceof TableScan))
                                 .noInputs())

                ).build();

        @Override
        default RelOptRule toRule() {
            return new UpdateNoScanLogicalRule(this);
        }
    }

    UpdateNoScanLogicalRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModify update = call.rel(0);
        RelNode input = call.rel(1);

        UpdateLogicalRel rel = new UpdateLogicalRel(
                update.getCluster(),
                OptUtils.toLogicalConvention(update.getTraitSet()),
                update.getTable(),
                update.getCatalogReader(),
                OptUtils.toLogicalInput(input),
                requireNonNull(update.getUpdateColumnList()),
                requireNonNull(update.getSourceExpressionList()),
                update.isFlattened(),
                null
        );
        call.transformTo(rel);
    }
}
