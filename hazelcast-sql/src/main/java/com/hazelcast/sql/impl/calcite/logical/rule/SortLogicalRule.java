/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.logical.rule;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.logical.rel.SortLogicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

public class SortLogicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new SortLogicalRule();

    private SortLogicalRule() {
        super(
            RelOptRule.operand(LogicalSort.class, Convention.NONE, RelOptRule.any()),
            RelFactories.LOGICAL_BUILDER,
            "SortLogicalRule"
        );
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // TODO: Sort with LIMIT/OFFSET will require different treatment>
        return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);

        final RelNode input = sort.getInput();

        RelNode convertedInput = convert(input, input.getTraitSet().plus(HazelcastConventions.LOGICAL).simplify());

        call.transformTo(new SortLogicalRel(
            sort.getCluster(),
            sort.getTraitSet().plus(HazelcastConventions.LOGICAL),
            convertedInput,
            sort.getCollation(),
            sort.offset,
            sort.fetch
        ));
    }
}
