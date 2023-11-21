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

package org.apache.calcite.plan.hep;

import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.List;

public class HazelcastHepProgramBuilder {

    private final List<HepInstruction> instructions = new ArrayList<>();

    public HazelcastHepProgramBuilder addRuleInstance(RelOptRule rule) {
        return addInstruction(new HepInstruction.RuleInstance(rule));
    }

    private HazelcastHepProgramBuilder addInstruction(HepInstruction instruction) {
        instructions.add(instruction);
        return this;
    }

    public HepProgram build() {
        return new HepProgram(instructions);
    }
}
