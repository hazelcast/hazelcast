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

package com.hazelcast.jet.sql.impl.opt.nojobshortcuts;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

import static org.apache.calcite.plan.Convention.NONE;

/**
 * Planner rule that matches `SELECT COUNT(* | non_null_field) FROM foo_imap`.
 * <p>
 * Such aggregation is translated to direct `map.size()` call which does not
 * involve starting of any job.
 */
public class MapSizeRule extends RelRule<RelRule.Config> {

    private static final Config RULE_CONFIG = Config.EMPTY
            .withDescription(MapSizeRule.class.getSimpleName())
            .withOperandSupplier(
                    b0 -> b0.operand(Aggregate.class)
                            .trait(NONE)
                            .oneInput(b1 -> b1.operand(TableScan.class)
                                    .predicate(scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class))
                                    .noInputs()));

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new MapSizeRule();

    private MapSizeRule() {
        super(RULE_CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        TableScan scan = call.rel(1);

        // no filter
        RelOptTable table = scan.getTable();
        HazelcastTable hzTable = table.unwrap(HazelcastTable.class);
        assert hzTable != null;
        if (hzTable.getFilter() != null) {
            return;
        }

        // no grouping
        if (!aggregate.getGroupSet().isEmpty() || aggregate.getGroupSets().size() != 1) {
            return;
        }
        if (aggregate.getAggCallList().size() != 1) {
            // we could support SELECT COUNT(*), COUNT(*), but we support only one aggregation.
            return;
        }
        AggregateCall aggCall = aggregate.getAggCallList().get(0);
        if (aggCall.getAggregation().getKind() != SqlKind.COUNT) {
            return;
        }
        if (aggCall.isDistinct()) {
            // we could support COUNT(DISTINCT __key), but we're lazy. Who would do that?
            return;
        }
        List<Integer> argList = aggCall.getArgList();
        if (argList.size() > 1) {
            return; // shouldn't happen, COUNT has only 1 argument (or 0 for COUNT(*))
        }
        if (argList.size() == 1 && scan.getRowType().getFieldList().get(argList.get(0)).getType().isNullable()) {
            return;
        }

        call.transformTo(new MapSizeRel(
                aggregate.getCluster(),
                OptUtils.toPhysicalConvention(aggregate.getTraitSet()),
                aggregate.getRowType(), table));
    }
}
