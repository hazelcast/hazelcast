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

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

public final class JoinConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new JoinConverterRule();

    protected JoinConverterRule() {
        super(Config.INSTANCE.withConversion(
                JoinNestedLoopPhysicalRel.class,
                PHYSICAL,
                PHYSICAL,
                JoinConverterRule.class.getSimpleName()));

    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        JoinNestedLoopPhysicalRel join = (JoinNestedLoopPhysicalRel) rel;
        RelSubset relSubset = (RelSubset) join.getRight();
        if (relSubset.getBest() instanceof TableScan) {
            return null;
        }

        return new JoinHashPhysicalRel(
                join.getCluster(),
                join.getTraitSet(),
                join.getLeft(),
                join.getRight(),
                join.getCondition(),
                join.getJoinType()
        );
    }
}
