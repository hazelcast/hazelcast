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

package com.hazelcast.jet.sql.impl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

/**
 * Performs query planning.
 */
public class QueryPlanner {

    private final VolcanoPlanner planner;

    public QueryPlanner(VolcanoPlanner planner) {
        this.planner = planner;
    }

    public RelNode optimize(RelNode node, RuleSet rules, RelTraitSet traitSet) {
        Program program = Programs.of(rules);

        return program.run(
                planner,
                node,
                traitSet,
                ImmutableList.of(),
                ImmutableList.of()
        );
    }
}
