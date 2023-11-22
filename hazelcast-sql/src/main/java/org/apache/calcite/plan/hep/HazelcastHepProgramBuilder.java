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
    private volatile boolean frozen;

    public HazelcastHepProgramBuilder addRuleInstance(RelOptRule rule) {
        if (!frozen) {
            return addInstruction(new HepInstruction.RuleInstance(rule));
        } else {
            throw new IllegalStateException("HepProgram instructions are frozen");
        }
    }

    /**
     * Freeze instruction set, making it immutable for public API
     * and ready to be used in {@link HepPlanner}.
     */
    public void freeze() {
        frozen = true;
    }

    private HazelcastHepProgramBuilder addInstruction(HepInstruction instruction) {
        instructions.add(instruction);
        return this;
    }

    /**
     * Returns the constructed program, and keep instructions for further use.
     */
    public HepProgram build() {
        return new HepProgram(instructions);
    }
}
