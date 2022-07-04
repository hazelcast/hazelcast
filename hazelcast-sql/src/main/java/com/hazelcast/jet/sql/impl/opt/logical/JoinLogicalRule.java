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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.JoinCommuteRule;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

final class JoinLogicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new JoinLogicalRule();

    private JoinLogicalRule() {
        super(
                LogicalJoin.class, Convention.NONE, LOGICAL,
                JoinLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;

        // We convert every RIGHT JOIN to LEFT JOIN to use already
        // implemented LEFT JOIN operators.
        if (join.getJoinType() == JoinRelType.RIGHT) {
            return JoinCommuteRule.swap(join, true);
        }

        return new JoinLogicalRel(
                join.getCluster(),
                OptUtils.toLogicalConvention(join.getTraitSet()),
                OptUtils.toLogicalInput(join.getLeft()),
                OptUtils.toLogicalInput(join.getRight()),
                join.getCondition(),
                join.getJoinType()
        );
    }
}
