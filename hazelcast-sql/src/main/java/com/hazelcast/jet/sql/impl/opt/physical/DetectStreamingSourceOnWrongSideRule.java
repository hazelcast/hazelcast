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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

public final class DetectStreamingSourceOnWrongSideRule extends RelRule<RelRule.Config> {
    static final RelOptRule INSTANCE;
    private static final Config RULE_CONFIG;

    static {
        RULE_CONFIG = RelRule.Config.EMPTY
                .withDescription(DetectStreamingSourceOnWrongSideRule.class.getSimpleName())
                .withOperandSupplier(
                        b -> b.operand(Join.class)
                                .trait(LOGICAL)
                                .predicate(DetectStreamingSourceOnWrongSideRule::matches)
                                .noInputs());
        INSTANCE = new DetectStreamingSourceOnWrongSideRule();
    }

    private DetectStreamingSourceOnWrongSideRule() {
        super(RULE_CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw QueryException.error(
                "The right side of a LEFT JOIN or the left side of a RIGHT JOIN cannot be a streaming source"
        );
    }

    private static boolean matches(Join join) {
        if (join.getJoinType() == JoinRelType.LEFT) {
            if (OptUtils.isUnbounded(join.getRight())) {
                return true;
            }
        }

        if (join.getJoinType() == JoinRelType.RIGHT) {
            return OptUtils.isUnbounded(join.getLeft());
        }

        return false;
    }


}
