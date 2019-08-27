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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.JoinLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * The rule that tries to created a collocated join.
 */
public class CollocatedJoinPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new CollocatedJoinPhysicalRule();

    private CollocatedJoinPhysicalRule() {
        super(
            RuleUtils.single(JoinLogicalRel.class, HazelcastConventions.LOGICAL),
            CollocatedJoinPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        // TODO: Iff this is an equijoin!
        // TODO: Iff top-level conjunction on collocated columns are present
        // TODO: Then a collocated join is applicable
    }
}
